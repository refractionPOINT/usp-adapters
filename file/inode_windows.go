//go:build windows
// +build windows

package usp_file

import (
	"os"
)

// getInodeFromFileInfo on Windows always returns 0 as Windows doesn't
// use inodes in the same way Unix-like systems do.
func getInodeFromFileInfo(fi os.FileInfo) uint64 {
	// Windows file system uses different mechanisms for file identity.
	// For now, we return 0 to indicate inode tracking is not available.
	return 0
}
