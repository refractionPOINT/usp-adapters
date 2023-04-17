//go:build !windows && !linux && !macos
// +build !windows,!linux,!macos

package main

func serviceMode(thisExe string, action string, args []string) error {
	return nil
}
