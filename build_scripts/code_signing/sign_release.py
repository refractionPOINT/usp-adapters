#!/usr/bin/env python3
"""Sign release binaries in place through client.py, in one signing request.

client.py sends an archive to the signing service, which signs each file in it
by platform: Windows by extension, macOS when "macos" is in the file name. A
macOS binary also needs entitlements, which the service looks up as
"<file name>.plist" inside an "entitlements.zip" at the archive root, so this
script builds that archive, has it signed, and writes each signed binary back
over the original (keeping the original file's mode).

The service account key comes from the CODE_SIGNING_KEY environment variable
(base64 encoded), never from the command line.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import subprocess
import sys
import tempfile
import zipfile

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CLIENT = os.path.join(SCRIPT_DIR, "client.py")
DEFAULT_ENTITLEMENTS = os.path.join(SCRIPT_DIR, "entitlements", "general.plist")


def sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def build_archive(files: list[str], entitlements: str, archive: str) -> None:
    with tempfile.TemporaryDirectory(prefix="entitlements_") as td:
        ent_zip = os.path.join(td, "entitlements.zip")
        with zipfile.ZipFile(ent_zip, "w", zipfile.ZIP_DEFLATED) as z:
            for f in files:
                name = os.path.basename(f)
                if "macos" in name:
                    z.write(entitlements, name + ".plist")
        with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as z:
            for f in files:
                z.write(f, os.path.basename(f))
            z.write(ent_zip, "entitlements.zip")


def extract_signed(archive: str, files: list[str]) -> None:
    with tempfile.TemporaryDirectory(prefix="signed_") as td:
        with zipfile.ZipFile(archive, "r") as z:
            z.extractall(td)
        found: dict[str, str] = {}
        for root, _dirs, names in os.walk(td):
            for name in names:
                found.setdefault(name, os.path.join(root, name))
        for f in files:
            name = os.path.basename(f)
            if name not in found:
                raise RuntimeError(f"{name} is missing from the signed archive")
            before = sha256(f)
            with open(found[name], "rb") as src, open(f, "wb") as dst:
                dst.write(src.read())
            if sha256(f) == before:
                raise RuntimeError(f"{name} came back unchanged: it was not signed")
            print(f"signed {name}: {before[:16]} -> {sha256(f)[:16]}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Sign release binaries in place.")
    parser.add_argument("files",
                        nargs="+",
                        help="binaries to sign (Windows .exe, or macOS with 'macos' in the name)")
    parser.add_argument("-e",
                        "--entitlements",
                        default=DEFAULT_ENTITLEMENTS,
                        help="plist applied to every macOS binary")
    parser.add_argument("--timeout",
                        type=int,
                        default=300,
                        help="seconds to wait for each signing attempt")
    args = parser.parse_args()

    key = os.environ.get("CODE_SIGNING_KEY", "")
    if not key:
        print("CODE_SIGNING_KEY is not set", file=sys.stderr)
        return 2
    files = [os.path.abspath(f) for f in args.files]
    for f in files:
        if not os.path.isfile(f):
            print(f"{f} not found", file=sys.stderr)
            return 2

    with tempfile.TemporaryDirectory(prefix="sign_release_") as td:
        archive = os.path.join(td, "release.zip")
        build_archive(files, args.entitlements, archive)
        rc = subprocess.run([sys.executable, CLIENT,
                             "--verbose",
                             "--timeout", str(args.timeout),
                             "--sign-type", "sensor",
                             "--base64-key", key,
                             "-i", archive],
                            check=False).returncode
        if rc != 0:
            print(f"client.py failed with exit code {rc}", file=sys.stderr)
            return rc
        extract_signed(archive, files)
    return 0


if __name__ == "__main__":
    sys.exit(main())
