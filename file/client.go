//go:build windows || darwin || linux || solaris || netbsd || openbsd || freebsd
// +build windows darwin linux solaris netbsd openbsd freebsd

package usp_file

import (
	"bytes"
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
}

type FileAdapter struct {
	ctx                   context.Context
	conf                  FileConfig
	wg                    sync.WaitGroup
	uspClient             *uspclient.Client
	writeTimeout          time.Duration
	tailFiles             map[string]*tailInfo
	mu                    sync.Mutex
	serialFeed            *semaphore.Weighted
	lineCb                func(line string) // callback for each line for testing
	inactivityThreshold   time.Duration
	reactivationThreshold time.Duration
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
	a := &FileAdapter{
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
	ctx, cancel := context.WithCancel(context.Background())
	a.ctx = ctx
	defer cancel()

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
				// Skip the tail/inactivity logic, but still detect rotation
				// (inode change) so a replaced file gets re-decoded.
				if info.processedAsParquet {
					if currentInode != 0 && info.inode != 0 && currentInode != info.inode {
						a.conf.ClientOptions.OnError(fmt.Errorf("[ROTATION DETECTED] Parquet file rotated: %s | old_inode=%d new_inode=%d | will reprocess",
							path, info.inode, currentInode))
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
			if _, ok := a.tailFiles[match]; !ok {
				stat, err := os.Stat(match)
				if err != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("error getting file stats: %v", err))
					continue
				}

				fileInode := getInodeFromFileInfo(stat)

				if a.inactivityThreshold > 0 && now.Sub(stat.ModTime()) > a.inactivityThreshold {
					a.conf.ClientOptions.OnWarning(fmt.Sprintf("[SKIP] File too old to open: %s | mtime=%s | age=%s",
						match, stat.ModTime().Format(time.RFC3339), now.Sub(stat.ModTime())))
					continue
				}

				// Parquet is binary and columnar — tailing it byte-for-byte
				// just ships unparseable garbage to the proxy. Detect it
				// here, decode the whole file once into newline-delimited
				// JSON, and feed each row through handleLine.
				isParquet, detectErr := detectParquetFile(match, stat.Size())
				if detectErr != nil {
					a.conf.ClientOptions.OnError(fmt.Errorf("parquet detect %s: %v", match, detectErr))
				}
				if isParquet {
					info := &tailInfo{
						lastActive:         time.Now(),
						inode:              fileInode,
						processedAsParquet: true,
					}
					a.tailFiles[match] = info
					a.wg.Add(1)
					go func(path string, info *tailInfo) {
						defer a.wg.Done()
						a.processParquetFile(path, info)
					}(match, info)
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
		time.Sleep(defaultPollingInterval)
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
	a.mu.Lock()
	for _, info := range a.tailFiles {
		if info.tail != nil {
			info.tail.Stop()
		}
	}
	a.mu.Unlock()
	err1 := a.uspClient.Drain(1 * time.Minute)
	_, err2 := a.uspClient.Close()

	if err1 != nil {
		return err1
	}

	return err2
}

// parquetMagic is the 4-byte signature at the start and end of a
// well-formed Parquet file.
var parquetMagic = []byte{'P', 'A', 'R', '1'}

// detectParquetFile returns true when the path is a Parquet file, either
// by .parquet extension or by PAR1 magic at both the header and trailer.
// Magic detection only needs 8 bytes off disk (4 from each end), so cheap
// enough to run once per newly-discovered file. The size argument lets the
// caller skip the open() entirely for files too short to carry valid magic.
func detectParquetFile(path string, size int64) (bool, error) {
	if strings.HasSuffix(strings.ToLower(path), ".parquet") {
		return true, nil
	}
	if size < 8 {
		return false, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()
	buf := make([]byte, 4)
	if _, err := f.ReadAt(buf, 0); err != nil {
		return false, err
	}
	if !bytes.Equal(buf, parquetMagic) {
		return false, nil
	}
	if _, err := f.ReadAt(buf, size-4); err != nil {
		return false, err
	}
	return bytes.Equal(buf, parquetMagic), nil
}

// processParquetFile reads a Parquet file in full, decodes it into
// newline-delimited JSON, and feeds each row through handleLine. The
// sentinel tailInfo is added to the map by the caller before this runs,
// which prevents the next poll cycle from re-discovering the file while
// decoding is still in progress.
func (a *FileAdapter) processParquetFile(path string, info *tailInfo) {
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("[NEW PARQUET] Decoding: %s | inode=%d", path, info.inode))

	data, err := os.ReadFile(path)
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("read parquet %s: %v", path, err))
		return
	}
	converted, err := utils.ParquetToJSONLines(data)
	if err != nil {
		a.conf.ClientOptions.OnError(fmt.Errorf("parquet decode %s: %v", path, err))
		return
	}

	var lineCount int64
	for line := range strings.SplitSeq(string(converted), "\n") {
		if line == "" {
			continue
		}
		a.handleLine(line)
		lineCount++
	}
	info.linesRead.Store(lineCount)
	info.bytesRead.Store(int64(len(converted)))
	atomic.StoreInt64(&info.lastData, time.Now().Unix())
	a.conf.ClientOptions.DebugLog(fmt.Sprintf("[PARQUET DONE] %s | rows=%d bytes=%d", path, lineCount, len(converted)))
}
