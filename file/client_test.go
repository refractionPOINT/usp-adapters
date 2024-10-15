package usp_file

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
