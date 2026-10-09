//go:build windows || darwin || linux || solaris || netbsd || openbsd || freebsd
// +build windows darwin linux solaris netbsd openbsd freebsd

package usp_file

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/refractionPOINT/go-uspclient/protocol"
	"github.com/refractionPOINT/usp-adapters/utils"

	"github.com/nxadm/tail"

	"golang.org/x/sync/semaphore"
)

const (
	defaultWriteTimeout          = 60 * 10
	defaultPollingInterval       = 10 * time.Second
	defaultReactivationThreshold = 60 * time.Second
	// maxParquetFileSize caps how large a single Parquet file we will
	// load into memory before decoding. Decoding happens in-process
	// against a full byte slice (the Apache Arrow Go reader needs
	// random access to the footer), so an unbounded read would let a
	// stray multi-GB drop OOM the adapter. 1 GiB is generous for
	// typical analytics drops and still safe on small EDR hosts.
	maxParquetFileSize = 1 << 30
	// staleHandleCooldown is how long a file that went stale is left with
	// no handle open before it is reopened. A Windows SMB client keeps a
	// closed file cached, stale EOF included, for about 10 seconds after
	// the last handle on it closes (measured: a reopen at 10s still saw the
	// old size, one at 12s the new one), and a reopen inside that window
	// revives the stale view and holds it again.
	staleHandleCooldown = 15 * time.Second
)

// getFileInode returns the inode number for a given file path.
// Returns 0 if the inode cannot be determined (e.g., on Windows or if file doesn't exist).
func getFileInode(path string) uint64 {
	stat, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return getInodeFromFileInfo(stat)
}

// contain a tail with a fields that help to now if a file is actively modified/in use
type tailInfo struct {
	tail       *tail.Tail
	lastActive time.Time
	isInactive bool
	lastOffset int64
	lastData   int64
	inode      uint64       // Track which inode is being tailed to detect rotation
	openStat   os.FileInfo  // the file as it was when the current tail opened it (see pinFileID)
	linesRead  atomic.Int64 // Counter for health monitoring
	bytesRead  atomic.Int64 // Counter for health monitoring
	// resumeOffset is the byte offset just past the last line (in
	// multi-line JSON mode, the last complete object) handed to
	// handleLine: where a replacement tail must start so that nothing is
	// skipped or shipped twice. tail.Tell() is not usable for this, since
	// with CompleteLines it already counts a partial line the tail is
	// holding back.
	resumeOffset atomic.Int64
	// handlerDone is closed when the handleInput goroutine reading the
	// current tail returns, i.e. once resumeOffset can no longer move.
	handlerDone chan struct{}
	// Stale-handle detection, owned by the poll loop (guarded by a.mu):
	// the file's mtime, size and our resumeOffset as of the previous poll.
	seenModTime time.Time
	seenSize    int64
	seenOffset  int64
	// reopening is set while a stale tail is being stopped off the poll
	// loop (releaseStale); the loop leaves the entry alone meanwhile.
	// released means it has stopped and nothing holds the file open; the
	// loop reopens it once staleCooldown has passed since releasedAt.
	reopening    bool
	released     bool
	releasedAt   time.Time
	staleReopens int
	// processedAsParquet marks entries created by the Parquet decode path.
	// These have no tail goroutine — the file was read once, decoded, and
	// emitted as JSON lines — so the poll loop must skip tail/inactivity
	// logic for them and only watch for inode-level rotation.
	processedAsParquet bool
	// parquetDecoding is true while the parquet decode goroutine is
	// running. The poll loop checks this before reacting to a rotated
	// inode so it doesn't race the in-flight decode (which would leave
	// two decode goroutines shipping the same file's rows).
	parquetDecoding atomic.Bool
	// parquetSize and parquetModTime record the file stat at the moment
	// the decode goroutine read the bytes. The poll loop compares the
	// current stat against these on later cycles so an in-place rewrite
	// (no inode change) still triggers a re-decode. Only meaningful
	// when processedAsParquet is true.
	parquetSize    int64
	parquetModTime time.Time
}

type FileAdapter struct {
	ctx                   context.Context
	cancel                context.CancelFunc // cancels ctx on Close so pollFiles exits and stops spawning new tail/parquet goroutines
	conf                  FileConfig
	wg                    sync.WaitGroup
	parquetWg             sync.WaitGroup // separate from wg so Close() can wait for parquet decodes without deadlocking on pollFiles' forever-loop
	uspClient             *uspclient.Client
	writeTimeout          time.Duration
	tailFiles             map[string]*tailInfo
	mu                    sync.Mutex
	serialFeed            *semaphore.Weighted
	lineCb                func(line string) // callback for each line for testing
	inactivityThreshold   time.Duration
	reactivationThreshold time.Duration
	// parquetMaxSize overrides the default maxParquetFileSize cap used
	// when reading and decompressing parquet files. Zero means "use the
	// default const". Tests set this to a small value to exercise the
	// cap without writing 1 GiB of data to disk.
	parquetMaxSize int64
	// staleCooldownOverride replaces staleHandleCooldown when non-zero, so
	// tests need not wait it out.
	staleCooldownOverride time.Duration
}

func (c *FileConfig) Validate() error {
	if err := c.ClientOptions.Validate(); err != nil {
		return fmt.Errorf("client_options: %v", err)
	}
	if c.FilePath == "" {
		return errors.New("file_path missing")
	}
	return nil
}

func NewFileAdapter(ctx context.Context, conf FileConfig) (*FileAdapter, chan struct{}, error) {
	// pollCtx is detached from the caller's ctx so the adapter has a
	// stable lifecycle gated only by Close(); the caller's ctx is still
	// used below for uspClient construction (existing behaviour).
	pollCtx, cancel := context.WithCancel(context.Background())
	a := &FileAdapter{
		ctx:        pollCtx,
		cancel:     cancel,
		conf:       conf,
		tailFiles:  make(map[string]*tailInfo),
		serialFeed: semaphore.NewWeighted(1),
	}

	if a.conf.WriteTimeoutSec == 0 {
		a.conf.WriteTimeoutSec = defaultWriteTimeout
	}
	a.writeTimeout = time.Duration(a.conf.WriteTimeoutSec) * time.Second

	var err error
	a.uspClient, err = uspclient.NewClient(ctx, conf.ClientOptions)
	if err != nil {
		cancel()
		return nil, nil, err
	}

	chStopped := make(chan struct{})
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		defer close(chStopped)
		a.pollFiles()
	}()

	return a, chStopped, nil
}

func (a *FileAdapter) pollFiles() {
	// Tests construct FileAdapter directly without going through
	// NewFileAdapter, so ctx may be nil here. Provide a fallback that
	// preserves the original lifecycle for those tests; production
	// code paths get their ctx wired up by NewFileAdapter.
	if a.ctx == nil {
		ctx, cancel := context.WithCancel(context.Background())
		a.ctx = ctx
		a.cancel = cancel
		defer cancel()
	}

	// Default inactivity threshold is 0 (disabled/never).
	// Only enable if explicitly set to a positive value in config.
	a.inactivityThreshold = time.Duration(a.conf.InactivityThreshold) * time.Second
	a.reactivationThreshold = defaultReactivationThreshold
	if a.conf.ReactivationThreshold != 0 {
		a.reactivationThreshold = time.Duration(a.conf.ReactivationThreshold) * time.Second
	}

	isFirstRun := true
	pollCycle := 0

	for {
		// Honour Close(): exit before doing any more work or spawning
		// new goroutines. The Sleep at the bottom of the loop also
		// uses ctx so a cancelled adapter shuts down promptly instead
		// of after a full poll interval.
		if a.ctx.Err() != nil {
			a.conf.ClientOptions.DebugLog("[POLL] context cancelled, exiting poll loop")
			return
		}

		pollCycle++
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("[POLL#%d] Starting poll cycle for pattern: %s", pollCycle, a.conf.FilePath))

		matches, err := filepath.Glob(a.conf.FilePath)
		if err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("glob error: %v", err))
			return
		}
		sort.Strings(matches)
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("[POLL#%d] Found %d matching files", pollCycle, len(matches)))

		a.mu.Lock()
		now := time.Now()

		// check all files against what we have in the tailfiles map
		for path, info := range a.tailFiles {
			if stat, err := os.Stat(path); err == nil {
				modTime := stat.ModTime()
				currentInode := getInodeFromFileInfo(stat)
				lastData := atomic.LoadInt64(&info.lastData)

				// Parquet files are decoded once and have no live tail.
				// Skip the tail/inactivity logic, but still detect a
				// re-write so the new content gets shipped:
				//   - inode change: classic rotation (rename + create)
				//   - same inode, different size or mtime: in-place
				//     rewrite (truncate + write or `>` redirect)
				// Only act once the in-flight decode (if any) has
				// finished — otherwise we'd spawn a second decode for
				// the same path while the first is still shipping rows.
				if info.processedAsParquet {
					if info.parquetDecoding.Load() {
						continue
					}
					rotated := currentInode != 0 && info.inode != 0 && currentInode != info.inode
					rewrittenInPlace := !rotated && (stat.Size() != info.parquetSize || !modTime.Equal(info.parquetModTime))
					if rotated {
						a.conf.ClientOptions.OnError(fmt.Errorf("[ROTATION DETECTED] Parquet file rotated: %s | old_inode=%d new_inode=%d | will reprocess",
							path, info.inode, currentInode))
						delete(a.tailFiles, path)
					} else if rewrittenInPlace {
						a.conf.ClientOptions.OnError(fmt.Errorf("[REWRITE DETECTED] Parquet file rewritten in place: %s | old_size=%d new_size=%d old_mtime=%s new_mtime=%s | will reprocess",
							path, info.parquetSize, stat.Size(), info.parquetModTime.Format(time.RFC3339), modTime.Format(time.RFC3339)))
						delete(a.tailFiles, path)
					}
					continue
				}

				// A stale tail is being stopped off this loop (releaseStale);
				// leave the entry alone until it has.
				if info.reopening {
					continue
				}
				// Released: nothing holds the file. Reopen once the client
				// can no longer be serving it from its cache.
				if info.released {
					if now.Sub(info.releasedAt) >= a.staleCooldown() {
						a.reopenReleased(path, info, stat)
					}
					continue
				}

				// Log detailed file state for debugging
				a.conf.ClientOptions.DebugLog(fmt.Sprintf("[POLL#%d] Checking file: %s | inode: tailed=%d current=%d | size=%d | mtime=%s | lastData=%s | inactive=%v",
					pollCycle, path, info.inode, currentInode, stat.Size(), modTime.Format(time.RFC3339),
					time.Unix(lastData, 0).Format(time.RFC3339), info.isInactive))

				// CRITICAL: Detect file rotation by inode change
				if currentInode != 0 && info.inode != 0 && currentInode != info.inode {
					a.conf.ClientOptions.OnError(fmt.Errorf("[ROTATION DETECTED] File rotated: %s | old_inode=%d new_inode=%d | Stopping old tail and will reopen",
						path, info.inode, currentInode))

					// Stop the old tail that's reading from the wrong inode
					// Note: We don't call Tell() here to avoid racing with the tail library's internal cleanup
					err := info.tail.Stop()
					if err != nil {
						a.conf.ClientOptions.OnError(fmt.Errorf("error stopping tail after rotation: %v", err))
					}
					info.tail.Cleanup()

					// Remove from map so the new file will be opened in the next section
					delete(a.tailFiles, path)
					continue
				}
				if info.isInactive {
					// validate if an inactive file has been modified recently and we need to tail it
					if now.Sub(modTime) <= a.reactivationThreshold {
						a.conf.ClientOptions.OnError(fmt.Errorf("[REACTIVATION] File reactivated: %s | inode=%d | restarting from beginning | mtime=%s",
							path, currentInode, modTime.Format(time.RFC3339)))

						// start from beginning (safer than trying to resume)
						t, err := tail.TailFile(path, a.tailConfig(&tail.SeekInfo{Offset: 0, Whence: io.SeekStart}))
						if err != nil {
							a.conf.ClientOptions.OnError(fmt.Errorf("tail error on reactivation: %v", err))
							continue
						}

						a.conf.ClientOptions.DebugLog(fmt.Sprintf("[REACTIVATION] Successfully reopened: %s | ReOpen=%v Follow=%v Poll=%v",
							path, !a.conf.NoFollow, !a.conf.NoFollow, a.conf.Poll))

						info.isInactive = false
						info.lastActive = modTime
						info.inode = currentInode // Update to current inode
						info.openStat = pinFileID(stat)
						info.seenModTime = time.Time{}
						a.startTail(info, t, 0)
					}
				} else {
					// validate if an active file has become inactive and we need to stop tailing it
					// We check for the last modified time AND we also check for the last time we saw
					// data flow from the file. We do this because Microsoft Windows sometimes decides to
					// stop updating the last modified time for things like IIS.
					timeSinceModTime := now.Sub(modTime)
					timeSinceLastData := now.Sub(time.Unix(lastData, 0))

					if a.inactivityThreshold > 0 && timeSinceModTime > a.inactivityThreshold && timeSinceLastData > a.inactivityThreshold {
						a.conf.ClientOptions.OnError(fmt.Errorf("[INACTIVITY] File inactive: %s | timeSinceMtime=%s timeSinceData=%s | threshold=%s",
							path, timeSinceModTime, timeSinceLastData, a.inactivityThreshold))

						// Note: We don't call Tell() here to avoid racing with the tail library's internal cleanup
						// Reactivation will start from offset 0, which is safe even if we miss some data
						a.conf.ClientOptions.DebugLog(fmt.Sprintf("[INACTIVITY] Stopping tail for %s (will restart from beginning if reactivated)", path))

						err := info.tail.Stop()
						if err != nil {
							a.conf.ClientOptions.OnError(fmt.Errorf("error stopping tail: %v", err))
						}
						info.isInactive = true
					} else if modTime.After(info.lastActive) {
						info.lastActive = modTime
					}

					// Health check: Warn if file hasn't produced data recently (potential stuck state)
					if timeSinceLastData > 2*time.Minute && lastData != 0 {
						a.conf.ClientOptions.OnWarning(fmt.Sprintf("[HEALTH] File hasn't produced data in %s: %s | lines=%d bytes=%d",
							timeSinceLastData, path, info.linesRead.Load(), info.bytesRead.Load()))
					}

					if !info.isInactive && a.isStaleHandle(info, stat) {
						a.releaseStale(path, info)
					}
				}
			} else {
				// Mid-release (releaseStale): look again next poll.
				if info.reopening {
					continue
				}
				// Released: its tail is already stopped.
				if info.released {
					a.conf.ClientOptions.OnError(fmt.Errorf("[REMOVAL] File removed from disk while released: %s", path))
					delete(a.tailFiles, path)
					continue
				}
				// Parquet entries have no live tail to stop; just drop them.
				if info.processedAsParquet {
					a.conf.ClientOptions.DebugLog(fmt.Sprintf("[REMOVAL] Parquet file removed from disk: %s | inode=%d | lines=%d",
						path, info.inode, info.linesRead.Load()))
					delete(a.tailFiles, path)
					continue
				}

				// file no longer exists on disk, close and remove all the tail resources
				a.conf.ClientOptions.OnError(fmt.Errorf("[REMOVAL] File removed from disk: %s | inode=%d | lines=%d bytes=%d",
					path, info.inode, info.linesRead.Load(), info.bytesRead.Load()))

				err := info.tail.Stop()
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("error stopping tail: %v", err))
				}
				info.tail.Cleanup()
				delete(a.tailFiles, path)

				a.conf.ClientOptions.DebugLog(fmt.Sprintf("[REMOVAL] Cleaned up resources for: %s", path))
			}
		}

		for _, match := range matches {
			// Don't spawn new tail/parquet goroutines once Close() has
			// signalled shutdown. parquetWg.Wait() in Close relies on
			// no new Add(1) happening after cancel; we synchronise on
			// a.mu (held throughout this loop) to make that safe.
			if a.ctx.Err() != nil {
				break
			}
			if _, ok := a.tailFiles[match]; !ok {
				stat, err := os.Stat(match)
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("error getting file stats: %v", err))
					continue
				}

				fileInode := getInodeFromFileInfo(stat)

				// Parquet detection runs *before* the inactivity-skip
				// check below: parquet drops are typically one-shot
				// daily/hourly files that may be older than the tail
				// inactivity threshold but still need decoding. Tailing
				// them byte-for-byte ships unparseable binary garbage,
				// so we always read+decode them in full when found.
				isParquet, detectErr := detectParquetFile(match)
				if detectErr != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("parquet detect %s: %v", match, detectErr))
				}
				if isParquet {
					info := &tailInfo{
						lastActive:         time.Now(),
						inode:              fileInode,
						processedAsParquet: true,
					}
					info.parquetDecoding.Store(true)
					a.tailFiles[match] = info
					a.parquetWg.Add(1)
					go func(path string, info *tailInfo) {
						defer a.parquetWg.Done()
						a.processParquetFile(path, info)
					}(match, info)
					continue
				}

				if a.inactivityThreshold > 0 && now.Sub(stat.ModTime()) > a.inactivityThreshold {
					a.conf.ClientOptions.OnWarning(fmt.Sprintf("[SKIP] File too old to open: %s | mtime=%s | age=%s",
						match, stat.ModTime().Format(time.RFC3339), now.Sub(stat.ModTime())))
					continue
				}

				// in general, tail existing files, but if a file appears after we started
				// (or we are backfilling) then start from the beginning of the file so as not to miss any data
				location := &tail.SeekInfo{Offset: 0, Whence: io.SeekEnd}
				startMode := "END"
				if a.conf.Backfill || !isFirstRun {
					location = &tail.SeekInfo{Offset: 0, Whence: io.SeekStart}
					startMode = "START"
				}

				a.conf.ClientOptions.DebugLog(fmt.Sprintf("[NEW FILE] Opening: %s | inode=%d | size=%d | mtime=%s | start=%s",
					match, fileInode, stat.Size(), stat.ModTime().Format(time.RFC3339), startMode))

				t, err := tail.TailFile(match, a.tailConfig(location))
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("tail error: %v", err))
					continue
				}

				a.conf.ClientOptions.DebugLog(fmt.Sprintf("[NEW FILE] Tail started: %s | ReOpen=%v Follow=%v Poll=%v",
					match, !a.conf.NoFollow, !a.conf.NoFollow, a.conf.Poll))

				info := &tailInfo{
					lastActive: time.Now(),
					isInactive: false,
					inode:      fileInode,
					openStat:   pinFileID(stat),
				}
				a.tailFiles[match] = info
				// A tail opened at the end starts where the file ended
				// when we looked; one opened at the start, at zero.
				startOffset := int64(0)
				if location.Whence == io.SeekEnd {
					startOffset = stat.Size()
				}
				a.startTail(info, t, startOffset)
			}
		}
		a.mu.Unlock()

		isFirstRun = false
		// select-on-Done so Close() doesn't have to wait a full poll
		// interval for the loop to wake up and notice the cancellation.
		select {
		case <-time.After(defaultPollingInterval):
		case <-a.ctx.Done():
			return
		}
	}
}

// tailConfig is the tail configuration every tail in this adapter uses;
// only where it starts reading differs.
func (a *FileAdapter) tailConfig(location *tail.SeekInfo) tail.Config {
	return tail.Config{
		ReOpen:        !a.conf.NoFollow,
		MustExist:     true,
		Follow:        !a.conf.NoFollow,
		CompleteLines: true,
		Poll:          a.conf.Poll,
		Location:      location,
	}
}

// startTail makes t the tail behind info and starts the goroutine that
// ships its lines. startOffset is the byte offset t begins reading at.
// Callers hold a.mu.
func (a *FileAdapter) startTail(info *tailInfo, t *tail.Tail, startOffset int64) {
	info.tail = t
	info.resumeOffset.Store(startOffset)
	info.seenOffset = startOffset
	done := make(chan struct{})
	info.handlerDone = done
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		defer close(done)
		a.handleInput(t, info)
	}()
}

// pinFileID returns stat after making it remember which file it describes.
// On Windows an os.FileInfo from os.Stat loads its volume and file index
// lazily, by path, the first time os.SameFile needs them; after a rename
// that path names a different file. Comparing stat with itself loads them
// now. Elsewhere this is a no-op.
func pinFileID(stat os.FileInfo) os.FileInfo {
	os.SameFile(stat, stat)
	return stat
}

// isStaleHandle reports whether the file changed since the previous poll
// (its mtime or size moved) while the tail shipped nothing.
//
// That is what a stale handle looks like on an SMB share. The Windows
// client caches an open file, end-of-file included, under the lease the
// server granted, and a write made on the server itself (not over SMB)
// does not break that lease, so reads keep hitting the old EOF. Unbuffered
// opens see the same EOF, and so does a new handle opened while the old
// one is still open or within ~10s of closing it: the client keeps the
// file cached that long after the last close. Depending on the server,
// the path's metadata shows the growth as a moving mtime with a frozen
// size, or the reverse; either counts as a change here.
//
// On a local file a false positive costs one release and reopen at the
// same offset (a write landing just before the poll, or a partial line).
// Callers hold a.mu.
func (a *FileAdapter) isStaleHandle(info *tailInfo, stat os.FileInfo) bool {
	offset := info.resumeOffset.Load()
	changed := !info.seenModTime.IsZero() &&
		(!stat.ModTime().Equal(info.seenModTime) || stat.Size() != info.seenSize)
	stale := changed && offset == info.seenOffset
	info.seenModTime = stat.ModTime()
	info.seenSize = stat.Size()
	info.seenOffset = offset
	return stale
}

// staleCooldown is how long a stale file is left with no handle open before
// it is reopened (see staleHandleCooldown). Tests shorten it.
func (a *FileAdapter) staleCooldown() time.Duration {
	if a.staleCooldownOverride > 0 {
		return a.staleCooldownOverride
	}
	return staleHandleCooldown
}

// releaseStale stops info's tail so that nothing holds the file open, and
// marks the entry released; the poll loop reopens it with reopenReleased
// once staleCooldown has passed. Stopping runs off the poll loop, because
// waiting for the old handler to drain can block for as long as a Ship()
// does. Callers hold a.mu.
func (a *FileAdapter) releaseStale(path string, info *tailInfo) {
	info.reopening = true
	info.staleReopens++
	msg := fmt.Sprintf("[STALE HANDLE] %s changed but nothing was read since the last poll | closing it for %s, then reopening at offset=%d (reopen #%d)",
		path, a.staleCooldown(), info.resumeOffset.Load(), info.staleReopens)
	// Reported once through OnError, like [ROTATION DETECTED]; a file that
	// keeps going stale would otherwise repeat it every poll.
	if info.staleReopens == 1 {
		a.conf.ClientOptions.OnError(errors.New(msg))
	} else {
		a.conf.ClientOptions.DebugLog(msg)
	}

	old, done := info.tail, info.handlerDone
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		// No old.Cleanup(): the stopping watcher already drops its watch,
		// and the watch is counted per file name, so a second removal
		// would leave the replacement tail with none ("If you plan to
		// re-read a file, don't call Cleanup in between" — tail.Cleanup).
		if err := old.Stop(); err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("error stopping stale tail: %v", err))
		}
		// Once the old handler has returned, resumeOffset is final.
		if done != nil {
			<-done
		}

		a.mu.Lock()
		defer a.mu.Unlock()
		info.reopening = false
		info.released = true
		info.releasedAt = time.Now()
	}()
}

// reopenReleased starts a fresh tail for a released entry at resumeOffset,
// the end of the last line shipped. Callers hold a.mu, and only call it
// from the poll loop, so a tail started here is always one Close() stops.
func (a *FileAdapter) reopenReleased(path string, info *tailInfo, stat os.FileInfo) {
	info.released = false
	offset := info.resumeOffset.Load()
	// A different or shorter file now sits at this path (a rotation that
	// inode tracking cannot see, as on Windows, or a rewrite): read it from
	// the start, as the tail itself would on a reopen.
	if !os.SameFile(info.openStat, stat) || stat.Size() < offset {
		offset = 0
	}
	t, err := tail.TailFile(path, a.tailConfig(&tail.SeekInfo{Offset: offset, Whence: io.SeekStart}))
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("[STALE HANDLE] tail error reopening %s: %v", path, err))
		delete(a.tailFiles, path)
		return
	}
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("[STALE HANDLE] reopened %s at offset=%d", path, offset))
	info.openStat = pinFileID(stat)
	info.seenModTime = time.Time{}
	a.startTail(info, t, offset)
}

func (a *FileAdapter) handleInput(t *tail.Tail, info *tailInfo) {
	filename := t.Filename
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("[TAIL START] Beginning to tail: %s", filename))
	pLastData := &info.lastData

	if a.conf.SerializeFiles {
		// If we are serializing files, we need to acquire a semaphore to ensure we only tail one file at a time.
		if err := a.serialFeed.Acquire(a.ctx, 1); err != nil {
			a.conf.ClientOptions.OnError(fmt.Errorf("error acquiring semaphore: %v", err))
			return
		}
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("starting file %s in serial mode", filename))
		defer a.serialFeed.Release(1)
	}

	// Periodic logging ticker
	logTicker := time.NewTicker(30 * time.Second)
	defer logTicker.Stop()

	lineCounter := 0
	logInterval := 100 // Log every 100 lines

	if !a.conf.MultiLineJSON {
		for {
			select {
			case line, ok := <-t.Lines:
				if !ok {
					// Channel closed
					a.conf.ClientOptions.DebugLog(fmt.Sprintf("[TAIL END] Lines channel closed for: %s | total_lines=%d total_bytes=%d",
						filename, info.linesRead.Load(), info.bytesRead.Load()))
					return
				}

				if line.Err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("[TAIL ERROR] tail.Line() error for %s: %v", filename, line.Err))
					return
				}

				atomic.StoreInt64(pLastData, time.Now().Unix())
				lineLen := int64(len(line.Text))
				info.linesRead.Add(1)
				info.bytesRead.Add(lineLen)

				lineCounter++
				if lineCounter%logInterval == 0 {
					offset, _ := t.Tell()
					a.conf.ClientOptions.DebugLog(fmt.Sprintf("[TAIL DATA] %s | lines=%d bytes=%d offset=%d",
						filename, info.linesRead.Load(), info.bytesRead.Load(), offset))
				}

				a.handleLine(line.Text)
				info.resumeOffset.Store(line.SeekInfo.Offset)

			case <-logTicker.C:
				// Periodic health log
				offset, _ := t.Tell()
				a.conf.ClientOptions.DebugLog(fmt.Sprintf("[TAIL HEALTH] %s | lines=%d bytes=%d offset=%d | inode=%d",
					filename, info.linesRead.Load(), info.bytesRead.Load(), offset, info.inode))
			}
		}
	} else {
		a.conf.ClientOptions.DebugLog(fmt.Sprintf("starting file %s in multi-line JSON mode", filename))
		var jsonLines []string
		braceCount := 0

		for {
			select {
			case line, ok := <-t.Lines:
				if !ok {
					a.conf.ClientOptions.DebugLog(fmt.Sprintf("[TAIL END] Lines channel closed for: %s | total_lines=%d total_bytes=%d",
						filename, info.linesRead.Load(), info.bytesRead.Load()))
					return
				}

				lineText := strings.TrimSpace(line.Text)
				if lineText == "" { // Skip empty lines.
					continue
				}
				jsonLines = append(jsonLines, lineText)
				braceCount += strings.Count(lineText, "{")
				braceCount -= strings.Count(lineText, "}")

				if braceCount == 0 && len(jsonLines) > 0 {
					rawJSON := []byte(strings.Join(jsonLines, ""))
					atomic.StoreInt64(pLastData, time.Now().Unix())
					info.linesRead.Add(1)
					info.bytesRead.Add(int64(len(rawJSON)))

					lineCounter++
					if lineCounter%logInterval == 0 {
						offset, _ := t.Tell()
						a.conf.ClientOptions.DebugLog(fmt.Sprintf("[TAIL DATA] %s | lines=%d bytes=%d offset=%d",
							filename, info.linesRead.Load(), info.bytesRead.Load(), offset))
					}

					a.handleLine(string(rawJSON))
					info.resumeOffset.Store(line.SeekInfo.Offset)
					jsonLines = nil // Reset for the next object.
				}

			case <-logTicker.C:
				offset, _ := t.Tell()
				a.conf.ClientOptions.DebugLog(fmt.Sprintf("[TAIL HEALTH] %s | lines=%d bytes=%d offset=%d | inode=%d",
					filename, info.linesRead.Load(), info.bytesRead.Load(), offset, info.inode))
			}
		}
	}
}

func (a *FileAdapter) handleLine(line string) {
	if len(line) == 0 {
		return
	}
	if a.lineCb != nil {
		a.lineCb(line)
	}
	msg := &protocol.DataMessage{
		TextPayload: line,
		TimestampMs: uint64(time.Now().UnixNano() / int64(time.Millisecond)),
	}
	err := a.uspClient.Ship(msg, a.writeTimeout)
	if err == uspclient.ErrorBufferFull {
		a.conf.ClientOptions.OnWarning("stream falling behind")
		err = a.uspClient.Ship(msg, 1*time.Hour)
	}
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("Ship(): %v", err))
	}
}

func (a *FileAdapter) Close() error {
	a.conf.ClientOptions.DebugLog("closing")

	// Cancel ctx first so pollFiles stops spawning new tail/parquet
	// goroutines on its next iteration; without this, Close() races
	// the poll loop and rows can ship to a closed uspClient.
	if a.cancel != nil {
		a.cancel()
	}

	// Acquire a.mu briefly: if pollFiles is mid-cycle, this blocks
	// until it releases the lock — at which point any goroutines it
	// was spawning have already incremented parquetWg, so the Wait
	// below sees them.
	a.mu.Lock()
	for _, info := range a.tailFiles {
		if info.tail != nil {
			info.tail.Stop()
		}
	}
	a.mu.Unlock()

	// Wait for any in-flight parquet decode goroutines to finish their
	// per-row Ship calls before draining and closing the uspClient.
	// `parquetWg` is separate from `a.wg` because `a.wg` tracks the
	// pollFiles forever-loop too; if Close cancels ctx that loop will
	// exit, so we then wait on `a.wg` for tail handlers and the poll
	// loop to drain. This mirrors the parquet path: tail goroutines
	// can also be mid-`handleLine→Ship` when Close is called.
	a.parquetWg.Wait()
	a.wg.Wait()

	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := a.uspClient.Close()

	if err1 != nil {
		return err1
	}

	return err2
}

// detectParquetFile returns true when the path is a Parquet file, either
// by .parquet extension (with optional .gz suffix) or by PAR1 magic at
// both the header and trailer. Magic detection re-stats the file inside
// the function so a concurrent writer can't shrink the file between the
// caller's stat and our ReadAt and have us read past EOF — partial-write
// errors are reported as "not parquet, retry next poll" rather than
// short-circuiting the file into the tail-as-text path with the wrong
// content.
func detectParquetFile(path string) (bool, error) {
	lower := strings.ToLower(path)
	if strings.HasSuffix(lower, ".parquet") {
		return true, nil
	}
	if base, isGz := strings.CutSuffix(lower, ".gz"); isGz && strings.HasSuffix(base, ".parquet") {
		return true, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return false, err
	}
	size := stat.Size()
	if size < 8 {
		return false, nil
	}
	buf := make([]byte, 4)
	if _, err := f.ReadAt(buf, 0); err != nil {
		// A short read here means the file changed under us; treat it
		// as "not parquet, retry next poll" rather than surfacing the
		// error and falling through to tail-as-text.
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return false, nil
		}
		return false, err
	}
	if !bytes.Equal(buf, utils.ParquetMagic) {
		return false, nil
	}
	if _, err := f.ReadAt(buf, size-4); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return false, nil
		}
		return false, err
	}
	return bytes.Equal(buf, utils.ParquetMagic), nil
}

// readParquetBytes loads the whole Parquet file into memory, peeling a
// gzip layer first when the path looks like *.parquet.gz (Athena UNLOAD
// and Firehose-to-Parquet both produce this). The sizeCap bounds both
// the raw file read and the post-gunzip plain-text size so a multi-GB
// drop or a gzip bomb can't OOM the adapter. Returns the file's size
// and mtime alongside the bytes so callers can detect later in-place
// rewrites without re-stat'ing.
func readParquetBytes(path string, sizeCap int64) (data []byte, size int64, modTime time.Time, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, time.Time{}, err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return nil, 0, time.Time{}, err
	}
	size = stat.Size()
	modTime = stat.ModTime()

	if size > sizeCap {
		return nil, size, modTime, fmt.Errorf("file size %d exceeds parquet cap %d", size, sizeCap)
	}

	// Bound the read end-to-end so we never allocate more than
	// sizeCap+1 bytes, regardless of TOCTOU between stat and read or
	// of the decompressed size when the path is gzipped. The cap is
	// applied at whichever stage produces the bytes the parquet
	// decoder will see — either the raw file, or the gunzipped stream
	// for *.parquet.gz objects.
	var src io.Reader
	if base, isGz := strings.CutSuffix(strings.ToLower(path), ".gz"); isGz && strings.HasSuffix(base, ".parquet") {
		gz, gzErr := gzip.NewReader(f)
		if gzErr != nil {
			return nil, size, modTime, fmt.Errorf("gunzip: %w", gzErr)
		}
		defer gz.Close()
		src = io.LimitReader(gz, sizeCap+1)
	} else {
		src = io.LimitReader(f, sizeCap+1)
	}

	data, err = io.ReadAll(src)
	if err != nil {
		return nil, size, modTime, err
	}
	if int64(len(data)) > sizeCap {
		return nil, size, modTime, fmt.Errorf("payload exceeds parquet cap %d", sizeCap)
	}
	return data, size, modTime, nil
}

// parquetSizeCap returns the size cap to apply when reading parquet
// files. Tests can override it via the parquetMaxSize field; production
// code paths (NewFileAdapter) leave the field zero and pick up the
// const default.
func (a *FileAdapter) parquetSizeCap() int64 {
	if a.parquetMaxSize > 0 {
		return a.parquetMaxSize
	}
	return maxParquetFileSize
}

// processParquetFile reads a Parquet file in full, decodes it into
// newline-delimited JSON, and feeds each row through handleLine. The
// sentinel tailInfo is added to the map (with parquetDecoding=true) by
// the caller before this runs, which prevents the next poll cycle from
// reacting to mid-decode rotation. On any failure the sentinel is
// removed (only if the slot still points at *this* info) so the next
// poll cycle can retry rather than silently blocking the file forever.
func (a *FileAdapter) processParquetFile(path string, info *tailInfo) {
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("[NEW PARQUET] Decoding: %s | inode=%d", path, info.inode))
	defer info.parquetDecoding.Store(false)

	data, size, modTime, err := readParquetBytes(path, a.parquetSizeCap())
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("read parquet %s: %v", path, err))
		a.dropTailEntryIfMatch(path, info)
		return
	}
	// Record what we actually read so the poll loop can detect a later
	// in-place rewrite (no inode change but size/mtime moved).
	info.parquetSize = size
	info.parquetModTime = modTime

	converted, err := utils.ParquetToJSONLines(data)
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("parquet decode %s: %v", path, err))
		a.dropTailEntryIfMatch(path, info)
		return
	}

	// Increment linesRead per row so health logs that hit mid-decode
	// (e.g. [ROTATION] / [REMOVAL]) report a meaningful count instead
	// of zero. bytes.SplitSeq avoids the string(converted) copy that
	// strings.SplitSeq would require, halving peak memory.
	for line := range bytes.SplitSeq(converted, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		a.handleLine(string(line))
		info.linesRead.Add(1)
	}
	info.bytesRead.Store(int64(len(converted)))
	atomic.StoreInt64(&info.lastData, time.Now().Unix())
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("[PARQUET DONE] %s | rows=%d bytes=%d", path, info.linesRead.Load(), len(converted)))
}

// dropTailEntryIfMatch removes a tailFiles entry under the adapter
// mutex, but only if the slot still points at the tailInfo we expect.
// This prevents a failed parquet decode goroutine from clobbering a
// freshly-registered entry that was put in place after the original
// file was removed and recreated. Without the identity check, the
// poll loop would silently spawn a third decode for the same path on
// the next cycle (sentinel gone → looks "new" again).
func (a *FileAdapter) dropTailEntryIfMatch(path string, info *tailInfo) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.tailFiles[path] == info {
		delete(a.tailFiles, path)
	}
}
