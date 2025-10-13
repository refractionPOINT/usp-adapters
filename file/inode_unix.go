//go:build linux || darwin || solaris || netbsd || openbsd || freebsd
// +build linux darwin solaris netbsd openbsd freebsd

package usp_file

import (
	"os"
	"syscall"
)

// getInodeFromFileInfo extracts the inode number from os.FileInfo.
// Returns 0 if the inode cannot be determined.
func getInodeFromFileInfo(fi os.FileInfo) uint64 {
	if stat, ok := fi.Sys().(*syscall.Stat_t); ok {
		return stat.Ino
	}
	return 0
}
