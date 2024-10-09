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

func Test_pollFiles(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "test")
	assert.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "test.log")
	_, err = os.Create(testFile)
	assert.NoError(t, err)

	mockClientOptions := new(MockClientOptions)

	adapter := &FileAdapter{
		conf: FileConfig{
			FilePath:      filepath.Join(tmpDir, "*.log"),
			ClientOptions: uspclient.ClientOptions{OnError: mockClientOptions.OnError},
		},
		tailFiles: make(map[string]*tailInfo),
	}

	mockClientOptions.On("OnError", mock.Anything).Return()

	done := make(chan bool)
	go func() {
		adapter.pollFiles()
		done <- true
	}()
	time.Sleep(16 * time.Second)

	assert.Len(t, adapter.tailFiles, 1)
	assert.Contains(t, adapter.tailFiles, testFile)

	time.Sleep(10 * time.Second)

	assert.True(t, adapter.tailFiles[testFile].isInactive)

	os.Remove(testFile)

	time.Sleep(10 * time.Second)

	assert.Len(t, adapter.tailFiles, 0)

	close(done)

	mockClientOptions.AssertExpectations(t)
}
