package usp_file

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
			ClientOptions:         uspclient.ClientOptions{OnError: mockClientOptions.OnError},
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

	debugReceived := []string{}

	mockClientOptions := new(MockClientOptions)
	dummyUSPClient, err := uspclient.NewClient(uspclient.ClientOptions{
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
					debugReceived = append(debugReceived, msg)
					time.Sleep(1 * time.Second)
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

		time.Sleep(10 * time.Second)

		adapter.mu.Lock()
		_, exists := adapter.tailFiles[testFile2]
		assert.False(t, exists)
		adapter.mu.Unlock()
	})

	// We will check the order of things was correct by checking the debug logs.
	// It's not ideal but will do the job.
	expectedDebug1 := []string{
		fmt.Sprintf("starting file %s in serial mode", testFile1),
		fmt.Sprintf("finished file %s in serial mode", testFile1),
		fmt.Sprintf("starting file %s in serial mode", testFile2),
		fmt.Sprintf("finished file %s in serial mode", testFile2),
	}
	expectedDebug2 := []string{
		fmt.Sprintf("starting file %s in serial mode", testFile2),
		fmt.Sprintf("finished file %s in serial mode", testFile2),
		fmt.Sprintf("starting file %s in serial mode", testFile1),
		fmt.Sprintf("finished file %s in serial mode", testFile1),
	}
	if expectedDebug1[0] != debugReceived[0] {
		assert.Equal(t, expectedDebug2, debugReceived)
	} else {
		assert.Equal(t, expectedDebug1, debugReceived)
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
	dummyUSPClient, err := uspclient.NewClient(uspclient.ClientOptions{
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
			ClientOptions:         uspclient.ClientOptions{OnError: mockClientOptions.OnError},
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
	close(receivedLines)

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
