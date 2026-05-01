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
	linesRead  atomic.Int64 // Counter for health monitoring
	bytesRead  atomic.Int64 // Counter for health monitoring
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

						t, err := tail.TailFile(path, tail.Config{
							ReOpen:        !a.conf.NoFollow,
							MustExist:     true,
							Follow:        !a.conf.NoFollow,
							CompleteLines: true,
							Poll:          a.conf.Poll,
							Location:      &tail.SeekInfo{Offset: 0, Whence: io.SeekStart}, // start from beginning (safer than trying to resume)
						})
						if err != nil {
							a.conf.ClientOptions.OnError(fmt.Errorf("tail error on reactivation: %v", err))
							continue
						}

						a.conf.ClientOptions.DebugLog(fmt.Sprintf("[REACTIVATION] Successfully reopened: %s | ReOpen=%v Follow=%v Poll=%v",
							path, !a.conf.NoFollow, !a.conf.NoFollow, a.conf.Poll))

						info.tail = t
						info.isInactive = false
						info.lastActive = modTime
						info.inode = currentInode // Update to current inode
						a.wg.Add(1)

						go func(t *tail.Tail) {
							defer a.wg.Done()
							a.handleInput(t, &info.lastData)
						}(t)
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
				}
			} else {
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

				t, err := tail.TailFile(match, tail.Config{
					ReOpen:        !a.conf.NoFollow,
					MustExist:     true,
					Follow:        !a.conf.NoFollow,
					CompleteLines: true,
					Poll:          a.conf.Poll,
					Location:      location,
				})
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("tail error: %v", err))
					continue
				}

				a.conf.ClientOptions.DebugLog(fmt.Sprintf("[NEW FILE] Tail started: %s | ReOpen=%v Follow=%v Poll=%v",
					match, !a.conf.NoFollow, !a.conf.NoFollow, a.conf.Poll))

				info := &tailInfo{
					tail:       t,
					lastActive: time.Now(),
					isInactive: false,
					inode:      fileInode,
				}
				a.tailFiles[match] = info
				a.wg.Add(1)
				go func(t *tail.Tail, inode uint64) {
					defer a.wg.Done()
					a.handleInput(t, &info.lastData)
				}(t, fileInode)
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

func (a *FileAdapter) handleInput(t *tail.Tail, pLastData *int64) {
	filename := t.Filename
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("[TAIL START] Beginning to tail: %s", filename))

	// Get the tailInfo for this file to update counters
	a.mu.Lock()
	info, exists := a.tailFiles[filename]
	a.mu.Unlock()

	if !exists {
		a.conf.ClientOptions.OnError(fmt.Errorf("[TAIL ERROR] tailInfo not found for: %s", filename))
		return
	}

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
