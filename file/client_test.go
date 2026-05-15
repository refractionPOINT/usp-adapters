package usp_file

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v15/parquet"
	"github.com/apache/arrow/go/v15/parquet/pqarrow"
	"github.com/refractionPOINT/go-uspclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
)

type MockClientOptions struct {
	mock.Mock
}

func (m *MockClientOptions) OnError(err error) {
	m.Called(err)
}

// TestPollFiles tests the pollFiles function with actual file operations
func TestPollFiles(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test files
	testFile1 := filepath.Join(tmpDir, "test1.log")
	testFile2 := filepath.Join(tmpDir, "test2.log")
	createTestFile(t, testFile1, "initial content 1")
	createTestFile(t, testFile2, "initial content 2")

	mockClientOptions := new(MockClientOptions)
	// Create FileAdapter instance
	adapter := &FileAdapter{
		conf: FileConfig{
			FilePath:              filepath.Join(tmpDir, "*.log"),
			InactivityThreshold:   5,
			ReactivationThreshold: 10,
			ClientOptions: uspclient.ClientOptions{
				OnError: mockClientOptions.OnError,
				DebugLog: func(msg string) {
					return
				},
			},
		},
		tailFiles: make(map[string]*tailInfo),
	}
	mockClientOptions.On("OnError", mock.Anything).Return()

	go adapter.pollFiles()

	// Each of the time.Sleep calls is intentional for the pollFiles goroutine to run and update the struct fields
	t.Run("NewFilesTailed", func(t *testing.T) {
		time.Sleep(100 * time.Millisecond)

		adapter.mu.Lock()
		assert.Contains(t, adapter.tailFiles, testFile1)
		assert.Contains(t, adapter.tailFiles, testFile2)
		assert.False(t, adapter.tailFiles[testFile1].isInactive)
		assert.False(t, adapter.tailFiles[testFile2].isInactive)
		adapter.mu.Unlock()
	})

	t.Run("FileBecomesInactive", func(t *testing.T) {
		time.Sleep(10 * time.Second)

		adapter.mu.Lock()
		assert.True(t, adapter.tailFiles[testFile1].isInactive)
		assert.True(t, adapter.tailFiles[testFile2].isInactive)
		adapter.mu.Unlock()
	})

	t.Run("InactiveFileReactivated", func(t *testing.T) {
		err := appendToFile(testFile1, "new content")
		assert.NoError(t, err)

		time.Sleep(10 * time.Second)

		adapter.mu.Lock()
		assert.False(t, adapter.tailFiles[testFile1].isInactive)
		assert.True(t, adapter.tailFiles[testFile2].isInactive) // This should still be inactive
		adapter.mu.Unlock()
	})

	t.Run("FileRemoved", func(t *testing.T) {
		err := os.Remove(testFile2)
		assert.NoError(t, err)

		time.Sleep(10 * time.Second)

		adapter.mu.Lock()
		_, exists := adapter.tailFiles[testFile2]
		assert.False(t, exists)
		adapter.mu.Unlock()
	})
}

func TestPollSerialFiles(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test files
	testFile1 := filepath.Join(tmpDir, "test1.log")
	testFile2 := filepath.Join(tmpDir, "test2.log")
	createTestFile(t, testFile1, "initial content 1")
	createTestFile(t, testFile2, "initial content 2")

	var debugMu sync.Mutex
	debugReceived := []string{}

	mockClientOptions := new(MockClientOptions)
	dummyUSPClient, err := uspclient.NewClient(context.Background(), uspclient.ClientOptions{
		TestSinkMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Create FileAdapter instance
	adapter := &FileAdapter{
		conf: FileConfig{
			FilePath:              filepath.Join(tmpDir, "*.log"),
			InactivityThreshold:   5,
			ReactivationThreshold: 10,
			SerializeFiles:        true,
			NoFollow:              true,
			ClientOptions: uspclient.ClientOptions{
				OnError: mockClientOptions.OnError,
				DebugLog: func(msg string) {
					debugMu.Lock()
					debugReceived = append(debugReceived, msg)
					debugMu.Unlock()
				},
			},
		},
		tailFiles:  make(map[string]*tailInfo),
		serialFeed: semaphore.NewWeighted(1),
		uspClient:  dummyUSPClient,
	}
	mockClientOptions.On("OnError", mock.Anything).Return()

	go adapter.pollFiles()

	time.Sleep(5 * time.Second)

	t.Run("FileRemoved", func(t *testing.T) {
		err := os.Remove(testFile2)
		assert.NoError(t, err)

		// Wait for poll cycle to detect removal (poll runs every 10s, add buffer)
		time.Sleep(12 * time.Second)

		adapter.mu.Lock()
		_, exists := adapter.tailFiles[testFile2]
		assert.False(t, exists)
		adapter.mu.Unlock()
	})

	// We will check the order of things was correct by checking the debug logs.
	// Check that key messages exist (not exact match since we added detailed logging).
	// Note: test2 gets removed mid-test, so we only check for test1.
	requiredSubstrings := []string{
		fmt.Sprintf("Opening: %s", testFile1),
		fmt.Sprintf("Opening: %s", testFile2),
		fmt.Sprintf("starting file %s in serial mode", testFile1),
	}

	for _, required := range requiredSubstrings {
		found := false
		debugMu.Lock()
		for _, debug := range debugReceived {
			if strings.Contains(debug, required) {
				found = true
				break
			}
		}
		debugMu.Unlock()
		assert.True(t, found, "Expected to find substring in debug logs: %s", required)
	}
}

func TestTailActiveFile(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test file with initial content
	testFile := filepath.Join(tmpDir, "active.log")
	createTestFile(t, testFile, "initial content\n")

	// Create channels to receive USP messages
	receivedLines := make(chan string, 100)
	mockClientOptions := new(MockClientOptions)
	dummyUSPClient, err := uspclient.NewClient(context.Background(), uspclient.ClientOptions{
		TestSinkMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create FileAdapter instance
	adapter := &FileAdapter{
		conf: FileConfig{
			FilePath:              filepath.Join(tmpDir, "*.log"),
			InactivityThreshold:   2, // Set high to prevent inactivity
			ReactivationThreshold: 1,
			Backfill:              true,
			ClientOptions: uspclient.ClientOptions{
				OnError: mockClientOptions.OnError,
				DebugLog: func(msg string) {
					return
				},
			},
		},
		tailFiles: make(map[string]*tailInfo),
		uspClient: dummyUSPClient,
		lineCb: func(line string) {
			receivedLines <- line
		},
	}
	mockClientOptions.On("OnError", mock.Anything).Return()

	// Start the adapter
	go adapter.pollFiles()
	time.Sleep(100 * time.Millisecond) // Let the adapter start tailing

	// Write additional content to the file over a few seconds
	expectedLines := []string{
		"initial content",
		"line added after 1 second",
		"line added after 2 seconds",
		"line added after 3 seconds",
		"line added after 4 seconds",
		"line added after 5 seconds",
		"line added after 6 seconds",
		"line added after 7 seconds",
		"line added after 8 seconds",
		"line added after 9 seconds",
		"line added after 10 seconds",
	}

	for i := 1; i < len(expectedLines); i++ {
		time.Sleep(1 * time.Second)
		err := appendToFile(testFile, expectedLines[i]+"\n")
		assert.NoError(t, err)
	}

	// Give some time for processing
	time.Sleep(1 * time.Second)

	// Verify all lines were received
	var receivedLinesSlice []string
	timeout := time.After(10 * time.Second)
collecting:
	for {
		select {
		case line := <-receivedLines:
			receivedLinesSlice = append(receivedLinesSlice, line)
			if len(receivedLinesSlice) == len(expectedLines) {
				break collecting
			}
		case <-timeout:
			t.Fatal("timeout waiting for lines")
		}
	}

	// Verify we got all expected lines in order
	assert.Equal(t, expectedLines, receivedLinesSlice)

	// Verify the file is still being tailed
	adapter.mu.Lock()
	info, exists := adapter.tailFiles[testFile]
	assert.True(t, exists)
	assert.False(t, info.isInactive)
	adapter.mu.Unlock()
}

func TestMultiLineJSON(t *testing.T) {
	// Create a temporary directory for test files
	tmpDir, err := os.MkdirTemp("", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test file with multi-line JSON content
	testFile := filepath.Join(tmpDir, "multiline.json")
	initialJSON := `{
		"event": "initial",
		"timestamp": "2024-01-01T00:00:00Z",
		"data": {
			"field1": "value1",
			"field2": 123
		}
	}`
	createTestFile(t, testFile, initialJSON+"\n")

	// Create channels to receive JSON messages
	receivedJSON := make(chan string, 100)
	mockClientOptions := new(MockClientOptions)
	dummyUSPClient, err := uspclient.NewClient(context.Background(), uspclient.ClientOptions{
		TestSinkMode: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create FileAdapter instance with MultiLineJSON enabled
	adapter := &FileAdapter{
		conf: FileConfig{
			FilePath:              filepath.Join(tmpDir, "*.json"),
			InactivityThreshold:   5,
			ReactivationThreshold: 1,
			MultiLineJSON:         true,
			Backfill:              true,
			ClientOptions: uspclient.ClientOptions{
				OnError: mockClientOptions.OnError,
				DebugLog: func(msg string) {
					return
				},
			},
		},
		tailFiles: make(map[string]*tailInfo),
		uspClient: dummyUSPClient,
		lineCb: func(line string) {
			receivedJSON <- line
		},
	}
	mockClientOptions.On("OnError", mock.Anything).Return()

	// Start the adapter
	go adapter.pollFiles()
	time.Sleep(100 * time.Millisecond) // Let the adapter start tailing

	// Write additional multi-line JSON objects
	additionalJSON := []string{
		`{
			"event": "second",
			"timestamp": "2024-01-01T00:00:01Z",
			"nested": {
				"deep": {
					"field": "value"
				}
			}
		}`,
		`{
			"event": "third",
			"timestamp": "2024-01-01T00:00:02Z",
			"array": [
				1,
				2,
				3
			]
		}`,
	}

	for _, jsonObj := range additionalJSON {
		time.Sleep(100 * time.Millisecond)
		err := appendToFile(testFile, jsonObj+"\n")
		assert.NoError(t, err)
	}

	// Give some time for processing
	time.Sleep(1 * time.Second)

	// Collect received JSON objects with timeout
	var receivedObjects []string
	expectedCount := 3 // initial + 2 additional
	timeout := time.After(5 * time.Second)
collectLoop:
	for len(receivedObjects) < expectedCount {
		select {
		case json := <-receivedJSON:
			// Remove whitespace for comparison
			compactJSON := strings.Join(strings.Fields(json), "")
			receivedObjects = append(receivedObjects, compactJSON)
		case <-timeout:
			break collectLoop
		}
	}

	// Prepare expected JSON objects (removing whitespace for comparison)
	expectedObjects := []string{
		strings.Join(strings.Fields(initialJSON), ""),
		strings.Join(strings.Fields(additionalJSON[0]), ""),
		strings.Join(strings.Fields(additionalJSON[1]), ""),
	}

	// Verify we got all expected JSON objects
	assert.Equal(t, len(expectedObjects), len(receivedObjects))
	for i, expected := range expectedObjects {
		assert.Equal(t, expected, receivedObjects[i])
	}

	// Verify the file is still being tailed
	adapter.mu.Lock()
	info, exists := adapter.tailFiles[testFile]
	assert.True(t, exists)
	assert.False(t, info.isInactive)
	adapter.mu.Unlock()
}

// TestFileRotationDetection tests the core file rotation detection logic
func TestFileRotationDetection(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "app.log")
	archiveFile := filepath.Join(tmpDir, "app.log.old")

	// Create initial file with content
	createTestFile(t, testFile, "line1\nline2\nline3\n")
	initialInode := getFileInode(testFile)

	// Set up log capture
	logCapture := &LogCapture{}
	receivedLines := make(chan string, 100)

	mockClientOptions := new(MockClientOptions)
	dummyUSPClient, err := uspclient.NewClient(context.Background(), uspclient.ClientOptions{
		TestSinkMode: true,
	})
	require.NoError(t, err)

	adapter := &FileAdapter{
		conf: FileConfig{
			FilePath:              filepath.Join(tmpDir, "*.log"),
			InactivityThreshold:   60, // High to prevent inactivity
			ReactivationThreshold: 10,
			Backfill:              true,
			ClientOptions: uspclient.ClientOptions{
				OnError: func(err error) {
					logCapture.Add(err.Error())
				},
				OnWarning: func(msg string) {
					logCapture.Add(msg)
				},
				DebugLog: func(msg string) {
					logCapture.Add(msg)
				},
			},
		},
		tailFiles: make(map[string]*tailInfo),
		uspClient: dummyUSPClient,
		lineCb: func(line string) {
			receivedLines <- line
		},
	}
	mockClientOptions.On("OnError", mock.Anything).Return()

	// Start adapter
	go adapter.pollFiles()
	time.Sleep(500 * time.Millisecond)

	// Collect initial lines
	var lines []string
	timeout := time.After(2 * time.Second)
collectInitial:
	for {
		select {
		case line := <-receivedLines:
			lines = append(lines, line)
			if len(lines) == 3 {
				break collectInitial
			}
		case <-timeout:
			break collectInitial
		}
	}

	assert.Equal(t, []string{"line1", "line2", "line3"}, lines, "Initial lines should be received")

	// Verify initial inode is tracked
	adapter.mu.Lock()
	info, exists := adapter.tailFiles[testFile]
	assert.True(t, exists, "File should be tracked")
	assert.Equal(t, initialInode, info.inode, "Initial inode should be tracked")
	adapter.mu.Unlock()

	// Perform rotation: mv app.log app.log.old && create new app.log
	newInode := rotateFile(t, testFile, archiveFile, "line4\nline5\n")
	assert.NotEqual(t, initialInode, newInode, "New file should have different inode")

	// Wait for rotation detection (poll cycle is 10s)
	assert.True(t, waitForRotationDetection(t, logCapture, testFile, 15*time.Second),
		"Rotation should be detected within 15 seconds")

	// Wait for new file to be opened
	assert.True(t, waitForNewFile(t, logCapture, testFile, 5*time.Second),
		"New file should be opened")

	// Collect lines from new file
	timeout = time.After(3 * time.Second)
collectNew:
	for {
		select {
		case line := <-receivedLines:
			lines = append(lines, line)
			if len(lines) == 5 {
				break collectNew
			}
		case <-timeout:
			break collectNew
		}
	}

	// Verify all lines received (old + new)
	assert.Equal(t, []string{"line1", "line2", "line3", "line4", "line5"}, lines,
		"All lines from both old and new file should be received")

	// Verify new inode is tracked
	time.Sleep(500 * time.Millisecond) // Allow poll cycle to complete
	adapter.mu.Lock()
	info, exists = adapter.tailFiles[testFile]
	assert.True(t, exists, "File should still be tracked after rotation")
	assert.Equal(t, newInode, info.inode, "New inode should be tracked after rotation")
	adapter.mu.Unlock()

	// Verify logs contain expected messages
	logs := logCapture.GetAll()
	foundRotation := false
	foundNewFile := false
	for _, log := range logs {
		if strings.Contains(log, "[ROTATION DETECTED]") && strings.Contains(log, testFile) {
			foundRotation = true
			// Verify old inode is logged (new inode will be whatever the rotated file has)
			assert.Contains(t, log, fmt.Sprintf("old_inode=%d", initialInode))
		}
		if strings.Contains(log, "[NEW FILE] Opening:") && strings.Contains(log, testFile) {
			foundNewFile = true
			// Just verify the log mentions an inode, don't check exact value as it may be
			// assigned by the tail library's reopen
			assert.Contains(t, log, "inode=")
		}
	}
	assert.True(t, foundRotation, "Rotation detection log should be present")
	assert.True(t, foundNewFile, "New file opening log should be present")
}

// TestFileRotationPreservesData tests that all data is preserved across rotation
func TestFileRotationPreservesData(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "data.log")
	archiveFile := filepath.Join(tmpDir, "data.log.old")

	// Create file with 50 lines
	var initialContent string
	for i := 1; i <= 50; i++ {
		initialContent += fmt.Sprintf("pre-rotation-line-%d\n", i)
	}
	createTestFile(t, testFile, initialContent)

	logCapture := &LogCapture{}
	receivedLines := make(chan string, 200)

	dummyUSPClient, err := uspclient.NewClient(context.Background(), uspclient.ClientOptions{
		TestSinkMode: true,
	})
	require.NoError(t, err)

	adapter := &FileAdapter{
		conf: FileConfig{
			FilePath:              filepath.Join(tmpDir, "*.log"),
			InactivityThreshold:   120,
			ReactivationThreshold: 10,
			Backfill:              true,
			ClientOptions: uspclient.ClientOptions{
				OnError: func(err error) {
					logCapture.Add(err.Error())
				},
				DebugLog: func(msg string) {
					logCapture.Add(msg)
				},
			},
		},
		tailFiles: make(map[string]*tailInfo),
		uspClient: dummyUSPClient,
		lineCb: func(line string) {
			receivedLines <- line
		},
	}

	go adapter.pollFiles()
	time.Sleep(1 * time.Second)

	// Collect first 50 lines
	var lines []string
	timeout := time.After(5 * time.Second)
	for len(lines) < 50 {
		select {
		case line := <-receivedLines:
			lines = append(lines, line)
		case <-timeout:
			t.Fatalf("Timeout collecting first 50 lines, got %d", len(lines))
		}
	}

	// Rotate file and add 50 more lines
	rotateFile(t, testFile, archiveFile, "")

	// Write 50 new lines to new file
	for i := 51; i <= 100; i++ {
		err := appendToFile(testFile, fmt.Sprintf("post-rotation-line-%d\n", i))
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond) // Throttle writes
	}

	// Wait for rotation detection
	assert.True(t, waitForRotationDetection(t, logCapture, testFile, 15*time.Second),
		"Rotation should be detected")

	// Collect remaining lines
	timeout = time.After(10 * time.Second)
	for len(lines) < 100 {
		select {
		case line := <-receivedLines:
			lines = append(lines, line)
		case <-timeout:
			break
		}
	}

	// Verify we got all 100 lines
	assert.Equal(t, 100, len(lines), "Should receive all 100 lines across rotation")

	// Verify no duplicates
	lineSet := make(map[string]bool)
	for _, line := range lines {
		if lineSet[line] {
			t.Errorf("Duplicate line detected: %s", line)
		}
		lineSet[line] = true
	}

	// Verify lines are in order (pre-rotation lines before post-rotation)
	preRotationEnd := -1
	postRotationStart := -1
	for i, line := range lines {
		if strings.HasPrefix(line, "pre-rotation-line-") {
			preRotationEnd = i
		}
		if strings.HasPrefix(line, "post-rotation-line-") && postRotationStart == -1 {
			postRotationStart = i
		}
	}

	if postRotationStart != -1 && preRotationEnd != -1 {
		assert.Less(t, preRotationEnd, postRotationStart,
			"All pre-rotation lines should come before post-rotation lines")
	}
}

// TestMultipleFileRotations tests multiple sequential rotations of the same file
func TestMultipleFileRotations(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "multi.log")

	createTestFile(t, testFile, "rotation0\n")

	logCapture := &LogCapture{}
	receivedLines := make(chan string, 50)

	dummyUSPClient, err := uspclient.NewClient(context.Background(), uspclient.ClientOptions{
		TestSinkMode: true,
	})
	require.NoError(t, err)

	adapter := &FileAdapter{
		conf: FileConfig{
			FilePath:            filepath.Join(tmpDir, "*.log"),
			InactivityThreshold: 120,
			Backfill:            true,
			ClientOptions: uspclient.ClientOptions{
				OnError: func(err error) {
					logCapture.Add(err.Error())
				},
				DebugLog: func(msg string) {
					logCapture.Add(msg)
				},
			},
		},
		tailFiles: make(map[string]*tailInfo),
		uspClient: dummyUSPClient,
		lineCb: func(line string) {
			receivedLines <- line
		},
	}

	go adapter.pollFiles()
	time.Sleep(500 * time.Millisecond)

	// Perform 3 rotations
	for rotation := 1; rotation <= 3; rotation++ {
		archiveFile := filepath.Join(tmpDir, fmt.Sprintf("multi.log.%d", rotation))
		rotateFile(t, testFile, archiveFile, fmt.Sprintf("rotation%d\n", rotation))

		// Wait for detection
		assert.True(t, waitForRotationDetection(t, logCapture, testFile, 15*time.Second),
			fmt.Sprintf("Rotation %d should be detected", rotation))

		logCapture.Clear()          // Clear for next rotation detection
		time.Sleep(2 * time.Second) // Allow new file to be opened and read
	}

	// Collect all lines
	time.Sleep(2 * time.Second)

	var lines []string
	// Drain channel with timeout (expect at least 4 lines: rotation0 + rotation1-3)
	timeout := time.After(5 * time.Second)
drainLoop:
	for {
		select {
		case line := <-receivedLines:
			lines = append(lines, line)
		case <-timeout:
			break drainLoop
		}
	}

	// Should have lines from all rotations
	assert.GreaterOrEqual(t, len(lines), 4, "Should have received lines from all rotations")

	// Verify we got lines from each rotation
	hasRotation0 := false
	hasRotation1 := false
	hasRotation2 := false
	hasRotation3 := false

	for _, line := range lines {
		if line == "rotation0" {
			hasRotation0 = true
		}
		if line == "rotation1" {
			hasRotation1 = true
		}
		if line == "rotation2" {
			hasRotation2 = true
		}
		if line == "rotation3" {
			hasRotation3 = true
		}
	}

	assert.True(t, hasRotation0, "Should have line from rotation 0")
	assert.True(t, hasRotation1, "Should have line from rotation 1")
	assert.True(t, hasRotation2, "Should have line from rotation 2")
	assert.True(t, hasRotation3, "Should have line from rotation 3")
}

// TestConcurrentFileRotations tests multiple files rotating at nearly the same time
func TestConcurrentFileRotations(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	file1 := filepath.Join(tmpDir, "file1.log")
	file2 := filepath.Join(tmpDir, "file2.log")
	file3 := filepath.Join(tmpDir, "file3.log")

	createTestFile(t, file1, "file1-pre\n")
	createTestFile(t, file2, "file2-pre\n")
	createTestFile(t, file3, "file3-pre\n")

	logCapture := &LogCapture{}
	receivedLines := make(chan string, 50)

	dummyUSPClient, err := uspclient.NewClient(context.Background(), uspclient.ClientOptions{
		TestSinkMode: true,
	})
	require.NoError(t, err)

	adapter := &FileAdapter{
		conf: FileConfig{
			FilePath:            filepath.Join(tmpDir, "*.log"),
			InactivityThreshold: 120,
			Backfill:            true,
			ClientOptions: uspclient.ClientOptions{
				OnError: func(err error) {
					logCapture.Add(err.Error())
				},
				DebugLog: func(msg string) {
					logCapture.Add(msg)
				},
			},
		},
		tailFiles: make(map[string]*tailInfo),
		uspClient: dummyUSPClient,
		lineCb: func(line string) {
			receivedLines <- line
		},
	}

	go adapter.pollFiles()
	time.Sleep(1 * time.Second)

	// Rotate all 3 files concurrently
	rotateFile(t, file1, filepath.Join(tmpDir, "file1.log.old"), "file1-post\n")
	rotateFile(t, file2, filepath.Join(tmpDir, "file2.log.old"), "file2-post\n")
	rotateFile(t, file3, filepath.Join(tmpDir, "file3.log.old"), "file3-post\n")

	// Wait for all rotations to be detected
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if logCapture.Contains("[ROTATION DETECTED] File rotated: "+file1) &&
			logCapture.Contains("[ROTATION DETECTED] File rotated: "+file2) &&
			logCapture.Contains("[ROTATION DETECTED] File rotated: "+file3) {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Collect lines
	time.Sleep(3 * time.Second)

	var lines []string
	// Drain channel with timeout (expect 6 lines: 3 pre + 3 post)
	timeout := time.After(5 * time.Second)
drainLoop:
	for {
		select {
		case line := <-receivedLines:
			lines = append(lines, line)
		case <-timeout:
			break drainLoop
		}
	}

	// Verify all files' data received
	hasFile1Pre := false
	hasFile1Post := false
	hasFile2Pre := false
	hasFile2Post := false
	hasFile3Pre := false
	hasFile3Post := false

	for _, line := range lines {
		if line == "file1-pre" {
			hasFile1Pre = true
		}
		if line == "file1-post" {
			hasFile1Post = true
		}
		if line == "file2-pre" {
			hasFile2Pre = true
		}
		if line == "file2-post" {
			hasFile2Post = true
		}
		if line == "file3-pre" {
			hasFile3Pre = true
		}
		if line == "file3-post" {
			hasFile3Post = true
		}
	}

	assert.True(t, hasFile1Pre && hasFile1Post, "File1 pre and post data should be received")
	assert.True(t, hasFile2Pre && hasFile2Post, "File2 pre and post data should be received")
	assert.True(t, hasFile3Pre && hasFile3Post, "File3 pre and post data should be received")

	// Verify all rotations logged
	assert.True(t, logCapture.Contains("[ROTATION DETECTED] File rotated: "+file1))
	assert.True(t, logCapture.Contains("[ROTATION DETECTED] File rotated: "+file2))
	assert.True(t, logCapture.Contains("[ROTATION DETECTED] File rotated: "+file3))
}

func createTestFile(t *testing.T, filename, content string) {
	err := os.WriteFile(filename, []byte(content), 0644)
	assert.NoError(t, err)
}

func appendToFile(filename, content string) error {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(content)
	return err
}

// Test helpers for rotation testing

// LogCapture captures log messages with thread-safety for testing
type LogCapture struct {
	mu       sync.Mutex
	messages []string
}

func (lc *LogCapture) Add(msg string) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.messages = append(lc.messages, msg)
}

func (lc *LogCapture) Contains(substring string) bool {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	for _, msg := range lc.messages {
		if strings.Contains(msg, substring) {
			return true
		}
	}
	return false
}

func (lc *LogCapture) GetAll() []string {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	result := make([]string, len(lc.messages))
	copy(result, lc.messages)
	return result
}

func (lc *LogCapture) Clear() {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.messages = nil
}

// rotateFile simulates log rotation (Zeek-style): rename old file, create new file
// Returns the new file's inode for verification
func rotateFile(t *testing.T, originalPath string, archivePath string, newContent string) uint64 {
	// Rename existing file to archive location
	err := os.Rename(originalPath, archivePath)
	require.NoError(t, err, "Failed to rotate file")

	// Create new file at original path
	createTestFile(t, originalPath, newContent)

	// Get and return new inode
	return getFileInode(originalPath)
}

// waitForRotationDetection waits for the adapter to detect rotation
func waitForRotationDetection(t *testing.T, logCapture *LogCapture, path string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if logCapture.Contains(fmt.Sprintf("[ROTATION DETECTED] File rotated: %s", path)) {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// waitForNewFile waits for a new file to be opened by the adapter
func waitForNewFile(t *testing.T, logCapture *LogCapture, path string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if logCapture.Contains(fmt.Sprintf("[NEW FILE] Opening: %s", path)) {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// buildTestParquet synthesises a small Parquet file for testing the
// adapter's decode path. Mirrors the helper in the utils package but is
// kept here so the file_test stays self-contained.
func buildTestParquet(t *testing.T, n int) []byte {
	t.Helper()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "event_type", Type: arrow.BinaryTypes.String},
		{Name: "session_id", Type: arrow.BinaryTypes.String},
		{Name: "user", Type: arrow.BinaryTypes.String},
	}, nil)

	mem := memory.DefaultAllocator
	rb := array.NewRecordBuilder(mem, schema)
	defer rb.Release()

	for i := 0; i < n; i++ {
		rb.Field(0).(*array.StringBuilder).Append("login.success")
		rb.Field(1).(*array.StringBuilder).Append(fmt.Sprintf("sess-%04d", i))
		rb.Field(2).(*array.StringBuilder).Append("alice")
	}

	rec := rb.NewRecord()
	defer rec.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{rec})
	defer tbl.Release()

	var buf bytes.Buffer
	if err := pqarrow.WriteTable(tbl, &buf, int64(n), parquet.NewWriterProperties(), pqarrow.DefaultWriterProps()); err != nil {
		t.Fatalf("pqarrow.WriteTable: %v", err)
	}
	return buf.Bytes()
}

func TestDetectParquetFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test-parquet-detect")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	parquetData := buildTestParquet(t, 1)

	// Extension-only: a file named .parquet is parquet even when we
	// don't peek inside.
	extPath := filepath.Join(tmpDir, "events.parquet")
	require.NoError(t, os.WriteFile(extPath, parquetData, 0644))

	got, err := detectParquetFile(extPath)
	require.NoError(t, err)
	assert.True(t, got)

	// Magic-only: extension is missing but the PAR1 header and trailer
	// give it away.
	magicPath := filepath.Join(tmpDir, "events.bin")
	require.NoError(t, os.WriteFile(magicPath, parquetData, 0644))

	got, err = detectParquetFile(magicPath)
	require.NoError(t, err)
	assert.True(t, got)

	// Plain text must not match.
	textPath := filepath.Join(tmpDir, "events.log")
	require.NoError(t, os.WriteFile(textPath, []byte("hello world\n"), 0644))

	got, err = detectParquetFile(textPath)
	require.NoError(t, err)
	assert.False(t, got)

	// Files smaller than the magic window must short-circuit to false.
	tinyPath := filepath.Join(tmpDir, "tiny.bin")
	require.NoError(t, os.WriteFile(tinyPath, []byte("PAR1"), 0644))

	got, err = detectParquetFile(tinyPath)
	require.NoError(t, err)
	assert.False(t, got)

	// .parquet.gz is the Athena UNLOAD / Firehose-to-Parquet pattern;
	// extension alone is enough — the gzip wrapping hides the magic.
	gzPath := filepath.Join(tmpDir, "events.parquet.gz")
	require.NoError(t, os.WriteFile(gzPath, gzipBytes(t, parquetData), 0644))

	got, err = detectParquetFile(gzPath)
	require.NoError(t, err)
	assert.True(t, got)
}

// gzipBytes wraps `data` in a gzip stream — used to build parquet.gz
// fixtures without committing binary blobs.
func gzipBytes(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write(data); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

// TestParquetFileIngestGzipped covers the *.parquet.gz path — the
// adapter must gunzip in-process before the parquet decoder sees the
// bytes, otherwise the proxy gets gzipped binary garbage the same way
// the original customer bug surfaced.
func TestParquetFileIngestGzipped(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test-parquet-gz")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	const rows = 4
	gzPath := filepath.Join(tmpDir, "events.parquet.gz")
	require.NoError(t, os.WriteFile(gzPath, gzipBytes(t, buildTestParquet(t, rows)), 0644))

	receivedLines := make(chan string, 100)
	dummyUSPClient, err := uspclient.NewClient(context.Background(), uspclient.ClientOptions{
		TestSinkMode: true,
	})
	require.NoError(t, err)
	mockClientOptions := new(MockClientOptions)
	mockClientOptions.On("OnError", mock.Anything).Return()

	adapter := &FileAdapter{
		conf: FileConfig{
			FilePath:            filepath.Join(tmpDir, "*"),
			InactivityThreshold: 60,
			Backfill:            true,
			ClientOptions: uspclient.ClientOptions{
				OnError:  mockClientOptions.OnError,
				DebugLog: func(msg string) {},
			},
		},
		tailFiles:  make(map[string]*tailInfo),
		serialFeed: semaphore.NewWeighted(1),
		uspClient:  dummyUSPClient,
		lineCb: func(line string) {
			receivedLines <- line
		},
	}

	go adapter.pollFiles()

	var collected []string
	timeout := time.After(5 * time.Second)
collect:
	for len(collected) < rows {
		select {
		case line := <-receivedLines:
			collected = append(collected, line)
		case <-timeout:
			break collect
		}
	}
	require.Len(t, collected, rows)
	for i, line := range collected {
		var row map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &row), "row %d not valid JSON: %q", i, line)
		assert.Equal(t, "login.success", row["event_type"])
	}
}

// TestParquetFileMalformed verifies that a file that *looks* like
// parquet (ends in .parquet) but fails to decode cleans up its sentinel
// so subsequent poll cycles can retry. Without this fix, a corrupt drop
// silently blocks the path forever.
func TestParquetFileMalformed(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test-parquet-bad")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// .parquet extension forces the decode path; the bytes are nonsense.
	badPath := filepath.Join(tmpDir, "bad.parquet")
	require.NoError(t, os.WriteFile(badPath, []byte("PAR1\x00\x00\x00\x00not-a-real-parquet-fileXXXXXXXXXXXXPAR1"), 0644))

	logs := &LogCapture{}
	dummyUSPClient, err := uspclient.NewClient(context.Background(), uspclient.ClientOptions{
		TestSinkMode: true,
	})
	require.NoError(t, err)

	adapter := &FileAdapter{
		conf: FileConfig{
			FilePath:            filepath.Join(tmpDir, "*"),
			InactivityThreshold: 60,
			Backfill:            true,
			ClientOptions: uspclient.ClientOptions{
				OnError:  func(err error) { logs.Add(err.Error()) },
				DebugLog: func(msg string) {},
			},
		},
		tailFiles:  make(map[string]*tailInfo),
		serialFeed: semaphore.NewWeighted(1),
		uspClient:  dummyUSPClient,
	}

	go adapter.pollFiles()

	// Wait long enough for the decode goroutine to run and clean up.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		adapter.mu.Lock()
		_, exists := adapter.tailFiles[badPath]
		adapter.mu.Unlock()
		if !exists && logs.Contains("parquet decode "+badPath) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	assert.True(t, logs.Contains("parquet decode "+badPath), "expected decode error to be logged")

	// Sentinel must be gone so the next poll cycle can retry; without
	// this the customer would have to restart or rotate the file.
	adapter.mu.Lock()
	_, exists := adapter.tailFiles[badPath]
	adapter.mu.Unlock()
	assert.False(t, exists, "decode failure should remove the sentinel")
}

// TestParquetFileRotation verifies that replacing a parquet file (new
// inode, same path) causes the adapter to re-decode the new content.
// Models log-rotation patterns common in the file adapter's other tests.
func TestParquetFileRotation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test-parquet-rot")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	parquetPath := filepath.Join(tmpDir, "events.parquet")
	require.NoError(t, os.WriteFile(parquetPath, buildTestParquet(t, 2), 0644))

	receivedLines := make(chan string, 100)
	dummyUSPClient, err := uspclient.NewClient(context.Background(), uspclient.ClientOptions{
		TestSinkMode: true,
	})
	require.NoError(t, err)
	mockClientOptions := new(MockClientOptions)
	mockClientOptions.On("OnError", mock.Anything).Return()

	adapter := &FileAdapter{
		conf: FileConfig{
			FilePath:            filepath.Join(tmpDir, "*"),
			InactivityThreshold: 60,
			Backfill:            true,
			ClientOptions: uspclient.ClientOptions{
				OnError:  mockClientOptions.OnError,
				DebugLog: func(msg string) {},
			},
		},
		tailFiles:  make(map[string]*tailInfo),
		serialFeed: semaphore.NewWeighted(1),
		uspClient:  dummyUSPClient,
		lineCb: func(line string) {
			receivedLines <- line
		},
	}

	go adapter.pollFiles()

	// Drain the first decode (2 rows).
	for i := 0; i < 2; i++ {
		select {
		case <-receivedLines:
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout on initial decode (got %d/%d)", i, 2)
		}
	}

	// Rotate: rename + write a fresh file with 3 rows. New inode.
	require.NoError(t, os.Rename(parquetPath, parquetPath+".old"))
	require.NoError(t, os.WriteFile(parquetPath, buildTestParquet(t, 3), 0644))

	// poll cycle is 10s; allow up to two cycles for rotation detection
	// + re-decode pickup.
	deadline := time.Now().Add(25 * time.Second)
	got := 0
	for time.Now().Before(deadline) && got < 3 {
		select {
		case <-receivedLines:
			got++
		case <-time.After(500 * time.Millisecond):
		}
	}
	assert.Equal(t, 3, got, "rotation should trigger re-decode of new file")
}

// TestParquetFileAlongsideTail makes sure adding parquet handling didn't
// break the basic mixed-dir case: a tailed log file and a parquet file
// in the same glob both work.
func TestParquetFileAlongsideTail(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test-parquet-mixed")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	logPath := filepath.Join(tmpDir, "events.log")
	require.NoError(t, os.WriteFile(logPath, []byte("text-line-1\ntext-line-2\n"), 0644))

	parquetPath := filepath.Join(tmpDir, "events.parquet")
	require.NoError(t, os.WriteFile(parquetPath, buildTestParquet(t, 2), 0644))

	receivedLines := make(chan string, 100)
	dummyUSPClient, err := uspclient.NewClient(context.Background(), uspclient.ClientOptions{
		TestSinkMode: true,
	})
	require.NoError(t, err)
	mockClientOptions := new(MockClientOptions)
	mockClientOptions.On("OnError", mock.Anything).Return()

	adapter := &FileAdapter{
		conf: FileConfig{
			FilePath:            filepath.Join(tmpDir, "*"),
			InactivityThreshold: 60,
			Backfill:            true,
			ClientOptions: uspclient.ClientOptions{
				OnError:  mockClientOptions.OnError,
				DebugLog: func(msg string) {},
			},
		},
		tailFiles:  make(map[string]*tailInfo),
		serialFeed: semaphore.NewWeighted(1),
		uspClient:  dummyUSPClient,
		lineCb: func(line string) {
			receivedLines <- line
		},
	}

	go adapter.pollFiles()

	// Expect 2 text + 2 parquet rows = 4 total.
	const want = 4
	collected := map[string]int{"text": 0, "json": 0}
	timeout := time.After(5 * time.Second)
	for collected["text"]+collected["json"] < want {
		select {
		case line := <-receivedLines:
			if strings.HasPrefix(line, "{") {
				collected["json"]++
			} else {
				collected["text"]++
			}
		case <-timeout:
			t.Fatalf("timeout: got %v", collected)
		}
	}
	assert.Equal(t, 2, collected["text"], "tailed log lines")
	assert.Equal(t, 2, collected["json"], "decoded parquet rows")
}

// TestParquetCloseWaitsForDecode locks down the Close()→Drain ordering
// fix: a parquet decode in flight must finish shipping before the
// uspClient is closed, otherwise the rows hit a closed Ship channel and
// disappear.
//
// The test is structured so it actively fails when parquetWg.Wait() is
// removed: each line sleeps a small amount AFTER the goroutine is
// unblocked, and the post-Close lineCount assertion runs *immediately*
// (no Eventually). Without the wait, Close returns the moment ctx is
// cancelled and lineCount is still mid-loop. With the wait, Close
// blocks until the goroutine drains every row.
func TestParquetCloseWaitsForDecode(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test-parquet-close")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	const (
		rows         = 50
		perLineSleep = 5 * time.Millisecond // 50 rows ≈ 250ms total post-release
	)
	parquetPath := filepath.Join(tmpDir, "events.parquet")
	require.NoError(t, os.WriteFile(parquetPath, buildTestParquet(t, rows), 0644))

	dummyUSPClient, err := uspclient.NewClient(context.Background(), uspclient.ClientOptions{
		TestSinkMode: true,
	})
	require.NoError(t, err)
	mockClientOptions := new(MockClientOptions)
	mockClientOptions.On("OnError", mock.Anything).Return()

	var lineCount atomic.Int64
	releaseSlowLines := make(chan struct{})
	adapter := &FileAdapter{
		conf: FileConfig{
			FilePath:            filepath.Join(tmpDir, "*"),
			InactivityThreshold: 60,
			Backfill:            true,
			ClientOptions: uspclient.ClientOptions{
				OnError:  mockClientOptions.OnError,
				DebugLog: func(msg string) {},
			},
		},
		tailFiles:  make(map[string]*tailInfo),
		serialFeed: semaphore.NewWeighted(1),
		uspClient:  dummyUSPClient,
		lineCb: func(line string) {
			n := lineCount.Add(1)
			if n == 5 {
				<-releaseSlowLines // park here so Close() races a stuck goroutine
			} else if n > 5 {
				time.Sleep(perLineSleep) // post-release: make remaining rows take real time
			}
		},
	}

	go adapter.pollFiles()

	require.Eventually(t, func() bool { return lineCount.Load() >= 5 }, 5*time.Second, 20*time.Millisecond,
		"decode goroutine should have started and parked at row 5")

	closed := make(chan error, 1)
	go func() { closed <- adapter.Close() }()

	// If Close() didn't wait, it would return immediately. While the
	// decode is parked waiting for releaseSlowLines, Close must block.
	select {
	case <-closed:
		t.Fatal("Close() returned before parquet decode goroutine finished (parquetWg.Wait missing?)")
	case <-time.After(200 * time.Millisecond):
	}
	require.Equal(t, int64(5), lineCount.Load(), "decode should still be parked at row 5")

	// Release. Without parquetWg.Wait, Close returns the moment ctx
	// cancellation propagates — well before the remaining 45 rows
	// (each sleeping perLineSleep) are processed.
	close(releaseSlowLines)

	var closeErr error
	select {
	case closeErr = <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close() did not return after decode goroutine was unblocked")
	}
	require.NoError(t, closeErr)

	// LOAD-BEARING: this runs in the same goroutine as Close return,
	// no Eventually polling. Without the wait, lineCount would still
	// be increasing in the background and this would fail.
	assert.Equal(t, int64(rows), lineCount.Load(), "every parquet row must ship before Close returns")
}

// TestParquetFileIngest covers the path the customer hit: pointing the
// file adapter at a directory of Parquet files and expecting one JSON
// event per row, not raw binary garbage. Reproduces the bug behind
// "invalid character 's' looking for beginning of value" before the fix.
func TestParquetFileIngest(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test-parquet-ingest")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	const rows = 5
	parquetPath := filepath.Join(tmpDir, "events.parquet")
	require.NoError(t, os.WriteFile(parquetPath, buildTestParquet(t, rows), 0644))

	receivedLines := make(chan string, 100)
	dummyUSPClient, err := uspclient.NewClient(context.Background(), uspclient.ClientOptions{
		TestSinkMode: true,
	})
	require.NoError(t, err)

	mockClientOptions := new(MockClientOptions)
	mockClientOptions.On("OnError", mock.Anything).Return()

	adapter := &FileAdapter{
		conf: FileConfig{
			FilePath:            filepath.Join(tmpDir, "*"),
			InactivityThreshold: 60,
			Backfill:            true,
			ClientOptions: uspclient.ClientOptions{
				OnError:  mockClientOptions.OnError,
				DebugLog: func(msg string) {},
			},
		},
		tailFiles:  make(map[string]*tailInfo),
		serialFeed: semaphore.NewWeighted(1),
		uspClient:  dummyUSPClient,
		lineCb: func(line string) {
			receivedLines <- line
		},
	}

	go adapter.pollFiles()

	var collected []string
	timeout := time.After(5 * time.Second)
collect:
	for len(collected) < rows {
		select {
		case line := <-receivedLines:
			collected = append(collected, line)
		case <-timeout:
			break collect
		}
	}

	require.Len(t, collected, rows, "expected %d rows decoded from parquet, got %d", rows, len(collected))

	// Each emitted line must be valid JSON with the columns from the
	// parquet schema — that's the contract the proxy depends on.
	for i, line := range collected {
		var row map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &row), "row %d not valid JSON: %q", i, line)
		assert.Equal(t, "login.success", row["event_type"], "row %d event_type", i)
		assert.Equal(t, "alice", row["user"], "row %d user", i)
		assert.NotEmpty(t, row["session_id"], "row %d session_id", i)
	}

	// Sentinel must be in the map and flagged as parquet so subsequent
	// poll cycles don't re-decode the file.
	adapter.mu.Lock()
	info, exists := adapter.tailFiles[parquetPath]
	require.True(t, exists)
	assert.True(t, info.processedAsParquet)
	assert.Nil(t, info.tail, "parquet sentinel must not own a tail")
	adapter.mu.Unlock()
}

// TestParquetSizeCap verifies that a parquet file larger than the
// configured cap is rejected without OOMing the adapter, and that the
// failed-decode sentinel is removed so the next poll can retry once
// the producer fixes the file size.
func TestParquetSizeCap(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test-parquet-cap")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	parquetPath := filepath.Join(tmpDir, "events.parquet")
	parquetData := buildTestParquet(t, 5)
	require.NoError(t, os.WriteFile(parquetPath, parquetData, 0644))

	logs := &LogCapture{}
	dummyUSPClient, err := uspclient.NewClient(context.Background(), uspclient.ClientOptions{TestSinkMode: true})
	require.NoError(t, err)

	adapter := &FileAdapter{
		conf: FileConfig{
			FilePath:            filepath.Join(tmpDir, "*"),
			InactivityThreshold: 60,
			Backfill:            true,
			ClientOptions: uspclient.ClientOptions{
				OnError:  func(err error) { logs.Add(err.Error()) },
				DebugLog: func(msg string) {},
			},
		},
		tailFiles:      make(map[string]*tailInfo),
		serialFeed:     semaphore.NewWeighted(1),
		uspClient:      dummyUSPClient,
		parquetMaxSize: 16, // tiny on purpose: real fixture is several KB
	}

	go adapter.pollFiles()

	// Wait for the cap error to surface and the sentinel to be removed.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		adapter.mu.Lock()
		_, exists := adapter.tailFiles[parquetPath]
		adapter.mu.Unlock()
		if !exists && logs.Contains("read parquet "+parquetPath) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	assert.True(t, logs.Contains("exceeds parquet cap"), "expected cap-violation error in logs")
	adapter.mu.Lock()
	_, exists := adapter.tailFiles[parquetPath]
	adapter.mu.Unlock()
	assert.False(t, exists, "cap violation should remove the sentinel so next poll can retry")
}

// TestParquetInPlaceRewrite verifies that overwriting a parquet file in
// place (same inode, new bytes) triggers a re-decode. Without size/mtime
// tracking the poll loop only watches inode and would silently keep
// shipping the original content.
func TestParquetInPlaceRewrite(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test-parquet-rewrite")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	parquetPath := filepath.Join(tmpDir, "events.parquet")
	require.NoError(t, os.WriteFile(parquetPath, buildTestParquet(t, 2), 0644))
	originalInode := getFileInode(parquetPath)

	receivedLines := make(chan string, 100)
	dummyUSPClient, err := uspclient.NewClient(context.Background(), uspclient.ClientOptions{TestSinkMode: true})
	require.NoError(t, err)
	mockClientOptions := new(MockClientOptions)
	mockClientOptions.On("OnError", mock.Anything).Return()

	adapter := &FileAdapter{
		conf: FileConfig{
			FilePath:            filepath.Join(tmpDir, "*"),
			InactivityThreshold: 60,
			Backfill:            true,
			ClientOptions: uspclient.ClientOptions{
				OnError:  mockClientOptions.OnError,
				DebugLog: func(msg string) {},
			},
		},
		tailFiles:  make(map[string]*tailInfo),
		serialFeed: semaphore.NewWeighted(1),
		uspClient:  dummyUSPClient,
		lineCb:     func(line string) { receivedLines <- line },
	}

	go adapter.pollFiles()

	for i := 0; i < 2; i++ {
		select {
		case <-receivedLines:
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout on initial decode (got %d/2)", i)
		}
	}

	// Overwrite in place: os.WriteFile truncates rather than unlinking,
	// so the inode is preserved on Linux. We assert that to be sure
	// we're really exercising the rewrite path, not a rotation.
	require.NoError(t, os.WriteFile(parquetPath, buildTestParquet(t, 4), 0644))
	// Bump mtime explicitly in case the filesystem mtime granularity
	// is too coarse to record the back-to-back writes.
	require.NoError(t, os.Chtimes(parquetPath, time.Now().Add(time.Second), time.Now().Add(time.Second)))
	require.Equal(t, originalInode, getFileInode(parquetPath), "expected in-place rewrite (same inode); test is invalid otherwise")

	// poll cycle is 10s; allow up to two cycles for rewrite detection
	// + re-decode.
	deadline := time.Now().Add(25 * time.Second)
	got := 0
	for time.Now().Before(deadline) && got < 4 {
		select {
		case <-receivedLines:
			got++
		case <-time.After(500 * time.Millisecond):
		}
	}
	assert.Equal(t, 4, got, "in-place rewrite should trigger re-decode of new content")
}
