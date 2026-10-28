#!/bin/bash
# Build a Windows amd64 binary that runs on Windows 7 / Server 2008 R2.
#
# Go 1.21+ requires Windows 10 / Server 2016, so this builds with the
# go-legacy-win7 toolchain (https://github.com/thongtech/go-legacy-win7), a
# community fork that tracks upstream Go releases and restores Windows 7
# support. It is not maintained by the Go team: the binary is best-effort.
#
# Trust: the release signs this binary with the project's code-signing key, so
# shipping it means trusting the fork's compiler and standard library, not only
# upstream Go. The checksum below guarantees the archive is the one that was
# reviewed when the version was pinned; it says nothing about how the fork
# built it. Review the fork's changes against upstream when bumping the version.
#
# Keep LEGACY_GO_VERSION at or above the go version in go.mod. GOTOOLCHAIN=local
# makes the build fail if it falls behind, rather than having Go silently
# download an upstream toolchain, which would produce a normal (Windows 10+)
# binary. The checksum is the SHA-256 published in the fork's release notes for
# the linux_amd64 archive.
#
# Usage: build_windows_legacy.sh <output.exe> <package>
set -euo pipefail

LEGACY_GO_VERSION='1.27.3-1'
LEGACY_GO_SHA256='069ceddac4917c528be492e560bb71bcf5c8ed5ff5d952ce1c8519e9b169228e'

if [ $# -ne 2 ]; then
  echo "usage: $0 <output.exe> <package>" >&2
  exit 1
fi
out="$1"
pkg="$2"

archive="go-legacy-win7-${LEGACY_GO_VERSION}.linux_amd64.tar.gz"
toolchain_dir="$(mktemp -d)"
trap 'rm -rf "$toolchain_dir"' EXIT

mkdir -p "$(dirname "$out")"
curl -fsSL --retry 5 --retry-all-errors --retry-delay 5 --max-time 300 -o "$toolchain_dir/$archive" \
  "https://github.com/thongtech/go-legacy-win7/releases/download/v${LEGACY_GO_VERSION}/${archive}"
echo "${LEGACY_GO_SHA256}  $toolchain_dir/$archive" | sha256sum -c -
tar -C "$toolchain_dir" -xzf "$toolchain_dir/$archive"

export GOROOT="$toolchain_dir/go-legacy-win7"
export PATH="$GOROOT/bin:$PATH"
export GOTOOLCHAIN=local
export GOOS=windows
export GOARCH=amd64

go version
go build -v -o "$out" "$pkg"
