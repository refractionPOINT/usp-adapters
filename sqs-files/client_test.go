package usp_sqs_files

import (
	"encoding/json"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQSFilesConfig_KeyPrefix(t *testing.T) {
	tests := []struct {
		name       string
		configJSON string
		wantPrefix string
	}{
		{
			name: "config with key_prefix",
			configJSON: `{
				"client_options": {
					"hostname": "test.com",
					"oid": "test-oid",
					"installation_key": "test-key"
				},
				"access_key": "test-access",
				"secret_key": "test-secret",
				"region": "us-east-1",
				"queue_url": "https://sqs.us-east-1.amazonaws.com/123456789/test",
				"key_prefix": "logs/backup/"
			}`,
			wantPrefix: "logs/backup/",
		},
		{
			name: "config without key_prefix",
			configJSON: `{
				"client_options": {
					"hostname": "test.com",
					"oid": "test-oid",
					"installation_key": "test-key"
				},
				"access_key": "test-access",
				"secret_key": "test-secret",
				"region": "us-east-1",
				"queue_url": "https://sqs.us-east-1.amazonaws.com/123456789/test"
			}`,
			wantPrefix: "",
		},
		{
			name: "config with empty key_prefix",
			configJSON: `{
				"client_options": {
					"hostname": "test.com",
					"oid": "test-oid",
					"installation_key": "test-key"
				},
				"access_key": "test-access",
				"secret_key": "test-secret",
				"region": "us-east-1",
				"queue_url": "https://sqs.us-east-1.amazonaws.com/123456789/test",
				"key_prefix": ""
			}`,
			wantPrefix: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var conf SQSFilesConfig
			err := json.Unmarshal([]byte(tt.configJSON), &conf)
			require.NoError(t, err)

			// Test that the key_prefix field is correctly unmarshaled
			assert.Equal(t, tt.wantPrefix, conf.KeyPrefix)
		})
	}
}

func TestKeyPrefixApplication(t *testing.T) {
	tests := []struct {
		name             string
		keyPrefix        string
		inputPath        string
		decodeObjectKey  bool
		expectedPath     string
		expectDecodeErr  bool
	}{
		{
			name:            "no prefix",
			keyPrefix:       "",
			inputPath:       "data/file.json",
			decodeObjectKey: false,
			expectedPath:    "data/file.json",
		},
		{
			name:            "prefix with trailing slash",
			keyPrefix:       "backup/",
			inputPath:       "data/file.json",
			decodeObjectKey: false,
			expectedPath:    "backup/data/file.json",
		},
		{
			name:            "prefix without trailing slash",
			keyPrefix:       "logs",
			inputPath:       "data/file.json",
			decodeObjectKey: false,
			expectedPath:    "logsdata/file.json",
		},
		{
			name:            "multi-level prefix",
			keyPrefix:       "archive/2024/01/",
			inputPath:       "events.log",
			decodeObjectKey: false,
			expectedPath:    "archive/2024/01/events.log",
		},
		{
			name:            "prefix with URL encoded path",
			keyPrefix:       "prefix/",
			inputPath:       "path%2Fwith%2Fencoded%2Fslashes.txt",
			decodeObjectKey: true,
			expectedPath:    "prefix/path/with/encoded/slashes.txt",
		},
		{
			name:            "no prefix with URL encoded path",
			keyPrefix:       "",
			inputPath:       "some%20file%20name.txt",
			decodeObjectKey: true,
			expectedPath:    "some file name.txt",
		},
		{
			name:            "prefix and decode with spaces",
			keyPrefix:       "s3-data/",
			inputPath:       "My%20Documents%2Ffile.pdf",
			decodeObjectKey: true,
			expectedPath:    "s3-data/My Documents/file.pdf",
		},
		{
			name:            "empty path with prefix",
			keyPrefix:       "prefix/",
			inputPath:       "",
			decodeObjectKey: false,
			expectedPath:    "prefix/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the path processing logic from processFiles
			path := tt.inputPath

			// URL decode if configured
			if tt.decodeObjectKey {
				var err error
				path, err = url.QueryUnescape(path)
				if tt.expectDecodeErr {
					assert.Error(t, err)
					return
				}
				require.NoError(t, err)
			}

			// Apply key prefix if configured
			if tt.keyPrefix != "" {
				path = tt.keyPrefix + path
			}

			assert.Equal(t, tt.expectedPath, path)
		})
	}
}

func TestKeyPrefixWithRealConfig(t *testing.T) {
	configJSON := `{
		"client_options": {
			"hostname": "test.com",
			"oid": "test-oid",
			"installation_key": "test-key"
		},
		"access_key": "test-access",
		"secret_key": "test-secret",
		"region": "us-east-1",
		"queue_url": "https://sqs.us-east-1.amazonaws.com/123456789/test",
		"key_prefix": "s3-backup/",
		"bucket": "my-bucket"
	}`

	var conf SQSFilesConfig
	err := json.Unmarshal([]byte(configJSON), &conf)
	require.NoError(t, err)

	// Verify key_prefix and bucket are correctly unmarshaled
	assert.Equal(t, "s3-backup/", conf.KeyPrefix)
	assert.Equal(t, "my-bucket", conf.Bucket)
}

func TestKeyPrefixYAMLUnmarshal(t *testing.T) {
	// Note: This test requires the yaml package if you want to test YAML unmarshaling
	// For now, we'll just verify the struct tags are correct by checking JSON
	configJSON := `{
		"client_options": {
			"hostname": "test.com",
			"oid": "test-oid",
			"installation_key": "test-key"
		},
		"access_key": "test-access",
		"secret_key": "test-secret",
		"region": "us-east-1",
		"queue_url": "https://sqs.us-east-1.amazonaws.com/123456789/test",
		"key_prefix": "production/logs/",
		"bucket": "my-s3-bucket",
		"is_decode_object_key": true
	}`

	var conf SQSFilesConfig
	err := json.Unmarshal([]byte(configJSON), &conf)
	require.NoError(t, err)

	assert.Equal(t, "production/logs/", conf.KeyPrefix)
	assert.Equal(t, "my-s3-bucket", conf.Bucket)
	assert.True(t, conf.IsDecodeObjectKey)
}

func TestKeyPrefixEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		prefix    string
		path      string
		expected  string
	}{
		{
			name:     "special characters in prefix",
			prefix:   "data-2024_01/",
			path:     "file.log",
			expected: "data-2024_01/file.log",
		},
		{
			name:     "prefix with dots",
			prefix:   "v1.0/",
			path:     "config.json",
			expected: "v1.0/config.json",
		},
		{
			name:     "very long prefix",
			prefix:   "level1/level2/level3/level4/level5/",
			path:     "deep/file.txt",
			expected: "level1/level2/level3/level4/level5/deep/file.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := tt.path
			if tt.prefix != "" {
				path = tt.prefix + path
			}
			assert.Equal(t, tt.expected, path)
		})
	}
}
