import base64
import glob
import hashlib
import logging
import os
import platform
import subprocess
import sys
import tarfile
import zipfile
from typing import Any


class LcUtil:

    @staticmethod
    def printkv(k: str, v: object) -> None:
        k = f"{k}:"
        print(f"    {k:30}{v}")

    @staticmethod
    def unzip(archive: str, output_dir: str) -> None:

        with zipfile.ZipFile(archive, "r") as z:
            for info in z.infolist():
                z.extract(info, output_dir)

                attr = info.external_attr >> 16

                if 0 != attr:
                    extracted_path = os.path.join(output_dir, info.filename)

                    if True == os.path.isfile(extracted_path):
                        os.chmod(extracted_path, attr)

    @staticmethod
    def zip(directory: str, out_file: str, include_root: bool = True) -> None:

        if True == include_root:
            rel_dir = os.path.dirname(directory)
        else:
            rel_dir = directory

        with zipfile.ZipFile(out_file, 'w', zipfile.ZIP_DEFLATED) as z:

            for root, _, files in os.walk(directory):
                for file in files:
                    file_path = os.path.join(root, file)
                    rel = os.path.relpath(file_path, rel_dir)
                    z.write(file_path, rel)

    @staticmethod
    def tar(directory: str, out_file: str, include_root: bool = True) -> None:

        if True == include_root:
            rel_dir = os.path.dirname(directory)
        else:
            rel_dir = directory

        with tarfile.open(out_file, "w:gz") as t:
            for root, _, files in os.walk(directory):
                for file in files:
                    file_path = os.path.join(root, file)
                    rel = os.path.relpath(file_path, rel_dir)
                    t.add(file_path, rel)

    @staticmethod
    def exec(cmd_line: str,
             cwd: str | None = None,
             env: dict[str, Any] | None = None,
             check: bool = True,
             capture_stdout: bool = True,
             stdin_data: str | None = None) -> tuple[int, str, str]:

        ret = 1
        out_str = ""
        out_err = ""

        env_copy = os.environ.copy()

        if env is not None:
            env_copy |= env

        if stdin_data is not None:
            stdin = subprocess.PIPE
        else:
            stdin = None

        if True == capture_stdout:
            p = subprocess.Popen(cmd_line,
                                 shell=True,
                                 text=True,
                                 env=env_copy,
                                 stdin=stdin,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE,
                                 cwd=cwd)

        else:
            p = subprocess.Popen(cmd_line,
                                 shell=True,
                                 text=True,
                                 stdin=stdin,
                                 env=env_copy,
                                 cwd=cwd)

        try:

            if stdin_data is not None:
                pass

            out_str, out_err = p.communicate(input=stdin_data)

            ret = p.returncode

            if True == check and 0 != ret:
                raise AssertionError(cmd_line, ret, out_str, out_err)
        finally:
            p.wait()

        return ret, out_str, out_err

    @staticmethod
    def size_fmt(num: float, suffix: str = "B") -> str:
        for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
            if abs(num) < 1024.0:
                return f"{num:3.1f} {unit}{suffix}"
            num /= 1024.0
        return f"{num:.1f} Yi{suffix}"

    @staticmethod
    def time_fmt(seconds: float) -> str:
        if seconds < 60:
            return f"{seconds} seconds"
        elif seconds < 3600:
            return f"{seconds / 60:.2f} minutes"
        elif seconds < 86400:
            return f"{seconds / 3600:.2f} hours"
        else:
            return f"{seconds / 86400:.2f} days"

    @staticmethod
    def file_size_fmt(file_path: str) -> str:
        return LcUtil.size_fmt(os.stat(file_path).st_size)

    @staticmethod
    def md5_file(file_path: str) -> str:
        hasher = hashlib.md5()

        with open(file_path, 'rb') as f:

            while True:
                chunk = f.read(8 * 1024)
                if b'' == chunk:
                    break
                hasher.update(chunk)

        return hasher.hexdigest()

    @staticmethod
    def get_local_lc_arch() -> str:

        arch = platform.machine()

        if "x86_64" == arch or "AMD64" == arch:
            return "x64"
        if "arm64" == arch:
            return "arm64"
        if "aarch64" == arch:
            return "arm64"

        raise NotImplementedError(f"Missing implementation for {arch}")

    @staticmethod
    def get_local_lc_plat() -> str:

        plat = sys.platform

        if "win32" == plat:
            return "win"
        if "darwin" == plat:
            return "osx"
        if "linux" == plat:
            return "linux"

        raise NotImplementedError(f"Missing implementation for {plat}")

    @staticmethod
    def b64_to_file(b64_string: str, file_path: str) -> None:
        with open(file_path, "wb+") as f:
            f.write(base64.b64decode(b64_string))

    @staticmethod
    def init_logging(log_file_path: str, verbose: bool = False):

        formatter = logging.Formatter(
            '%(asctime)-18s - %(levelname)s - %(message)s')

        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        file_handler = logging.FileHandler(log_file_path)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        if verbose:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

    @staticmethod
    def clock_pache_path() -> str:

        cur_root = os.path.dirname(sys.argv[0])
        clock_cache: str | None = None

        root_name = os.path.abspath(os.sep)

        # find the root dir first
        while cur_root != root_name:

            cur_dir_cache = os.path.join(cur_root, ".clock")

            if True == os.path.isdir(cur_dir_cache):
                clock_cache = cur_dir_cache
                break

            cur_root = os.path.join(cur_root, os.pardir)
            cur_root = os.path.abspath(cur_root)

        if clock_cache is None:
            raise FileNotFoundError("couldn't find .clock cache directory")

        return clock_cache

    @staticmethod
    def find_in_clock_cache(file_name: str) -> list[str]:

        cwd_file_path = os.path.join(os.getcwd(), file_name)

        if os.path.exists(cwd_file_path):
            return [cwd_file_path]

        clock_cache = LcUtil.clock_pache_path()

        files_patt = os.path.join(clock_cache, "**", file_name)

        return glob.glob(files_patt, recursive=True)
