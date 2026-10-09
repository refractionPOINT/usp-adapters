#!/usr/bin/env -S uv run --script
# /// script
# dependencies = []
# ///
import argparse
import base64
import enum
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass

script_root = os.path.abspath(os.path.dirname(sys.argv[0]))
lc_py_root = os.path.abspath(os.path.join(script_root, os.path.pardir))
sys.path.append(lc_py_root)

from lc_py.lc_config import JSONConfig  # nopep8
from lc_py.lc_py_utils import LcUtil  # nopep8

DEF_SIGN_TIMEOUT = (20 * 60)
DEF_SIGN_RETRIES = 3

IS_WINDOWS = os.name == "nt"


class SignFileType(enum.Enum):
    SENSOR_ARCHIVE = 1
    PACKAGE_ARCHIVE = 2
    HLK_ARCHIVE = 3


@dataclass
class SigningRequest:
    file_type: int
    unsigned_uri: str
    signed_uri: str


class InvalidArgumentError(Exception):
    pass


class GcloudError(Exception):

    def __init__(self, returncode: int, cmd: list[str],
                 stdout: bytes, stderr: bytes) -> None:
        self.returncode = returncode
        self.cmd = cmd
        self.stdout = stdout
        self.stderr = stderr
        msg = (f"gcloud {' '.join(cmd[1:])} exited {returncode}: "
               f"{stderr.decode(errors='replace').strip()}")
        super().__init__(msg)


def _resolve_gcloud() -> str:
    path = shutil.which("gcloud")
    if path is None and IS_WINDOWS:
        path = shutil.which("gcloud.cmd")
    if path is None:
        raise RuntimeError("gcloud CLI not found in PATH")
    return path


def _long_path(p: str) -> str:
    """Expand Windows 8.3 short-name path components (e.g. RUNNER~1) to their
    long form. gcloud storage's glob matcher fails to resolve short paths even
    though Python can open them, so paths handed to gcloud must be long form.
    No-op on POSIX."""
    if not IS_WINDOWS:
        return p
    import ctypes
    buf = ctypes.create_unicode_buffer(32768)
    n = ctypes.windll.kernel32.GetLongPathNameW(p, buf, 32768)  # type: ignore
    if 0 < n < 32768:
        return buf.value
    return p


def _kill_tree(proc: subprocess.Popen) -> None:
    if IS_WINDOWS:
        subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                       stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL,
                       timeout=10)
    else:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass


def _run_with_timeout(cmd: list[str], timeout: float,
                      env: dict[str, str] | None = None) -> tuple[int, bytes, bytes]:
    """Run cmd with a hard timeout.

    On timeout, the whole process tree is killed — taskkill /T on Windows,
    killpg on POSIX — so a hung gcloud (or its Python child) can't outlive us.
    """
    if IS_WINDOWS:
        # CREATE_NEW_PROCESS_GROUP is Windows-only; use getattr so this
        # module still imports on POSIX (where pyright/mypy also can't see it).
        creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        proc = subprocess.Popen(cmd,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                env=env,
                                creationflags=creationflags)
    else:
        proc = subprocess.Popen(cmd,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                env=env,
                                start_new_session=True)

    try:
        out, err = proc.communicate(timeout=max(0.1, timeout))
    except subprocess.TimeoutExpired:
        _kill_tree(proc)
        try:
            out, err = proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            out, err = b"", b""
        raise subprocess.TimeoutExpired(cmd, timeout,
                                        output=out, stderr=err)

    return proc.returncode, out, err


class SigningPublisher:

    def __init__(self, project: str, topic: str, bucket_name: str, key: str) -> None:
        self.project = project
        self.topic = topic
        self.bucket_name = bucket_name
        self.key = key
        self.gcloud = _resolve_gcloud()
        self._config_dir: str | None = None
        self.env: dict[str, str] | None = None

    def __enter__(self) -> 'SigningPublisher':
        logging.info(f"authenticating using {self.key}")

        self._config_dir = tempfile.mkdtemp(prefix="gcloud_cfg_")

        env = dict(os.environ)
        env["CLOUDSDK_CONFIG"] = self._config_dir
        env["CLOUDSDK_CORE_PROJECT"] = self.project
        env["CLOUDSDK_CORE_DISABLE_PROMPTS"] = "1"
        env["CLOUDSDK_COMPONENT_MANAGER_DISABLE_UPDATE_CHECK"] = "1"
        self.env = env

        rc, out, err = _run_with_timeout(
            [self.gcloud, "auth", "activate-service-account",
             "--key-file", self.key, "--quiet"],
            timeout=60.0,
            env=self.env)

        if rc != 0:
            raise GcloudError(rc,
                              [self.gcloud, "auth", "activate-service-account"],
                              out, err)

        return self

    def __exit__(self, exc_type: type[BaseException] | None,
                 exc_value: BaseException | None, traceback: object) -> None:
        if self._config_dir is not None:
            shutil.rmtree(self._config_dir, ignore_errors=True)

    def _gcloud(self, args: list[str], timeout: float) -> tuple[int, bytes, bytes]:
        cmd = [self.gcloud, *args]
        return _run_with_timeout(cmd, timeout=timeout, env=self.env)

    def _gcloud_checked(self, args: list[str], timeout: float) -> tuple[bytes, bytes]:
        rc, out, err = self._gcloud(args, timeout)
        if rc != 0:
            raise GcloudError(rc, [self.gcloud, *args], out, err)
        return out, err

    def _object_exists(self, uri: str, timeout: float) -> bool:
        rc, _out, _err = self._gcloud(
            ["storage", "objects", "describe", uri,
             "--format=value(name)"],
            timeout=timeout)
        return rc == 0

    def _upload(self, local_path: str, uri: str, timeout: float) -> None:
        src = _long_path(os.path.abspath(local_path))
        logging.info(f"uploading {src} to {uri}")
        self._gcloud_checked(
            ["storage", "cp", src, uri],
            timeout=timeout)

    def _download(self, uri: str, local_path: str, timeout: float) -> None:
        # Download to a temp file then move — gcloud storage cp can leave a
        # partial file on failure, and callers expect an atomic replace.
        # GetLongPathNameW only resolves existing paths, so resolve the dir
        # first and then join the filename.
        td = _long_path(tempfile.mkdtemp(prefix="gsi_storage_"))
        try:
            out_file = os.path.join(td, "download.bin")
            logging.info(f"downloading {uri} to {local_path}")
            self._gcloud_checked(
                ["storage", "cp", uri, out_file],
                timeout=timeout)
            shutil.move(out_file, local_path)
        finally:
            shutil.rmtree(td, ignore_errors=True)

    def _delete(self, uri: str, timeout: float) -> None:
        logging.info(f"deleting {uri}")
        rc, _out, err = self._gcloud(
            ["storage", "rm", uri, "--quiet"],
            timeout=timeout)
        if rc != 0:
            logging.info(
                f"delete {uri} returned {rc}: "
                f"{err.decode(errors='replace').strip()}")

    def _publish(self, message_bytes: bytes, timeout: float) -> str:
        # base64 is ASCII so round-tripping through gcloud's UTF-8 --message
        # preserves bytes verbatim.
        message_str = message_bytes.decode("ascii")
        out, _err = self._gcloud_checked(
            ["pubsub", "topics", "publish", self.topic,
             f"--project={self.project}",
             f"--message={message_str}",
             "--format=value(messageIds)"],
            timeout=timeout)
        return out.decode(errors="replace").strip()

    def sign_archive(self, sign_type: SignFileType, input: str, output: str, timeout: float, retries: int) -> None:

        logging.info(f"Input file: {input}")
        logging.info(f"Output file: {output}")

        GCS_OP_TIMEOUT = 120.0
        CLEANUP_TIMEOUT = 30.0
        PUBLISH_TIMEOUT = 30.0

        # Overall wall-clock budget: one upload + retries * per-attempt polling
        # window + small slop for publishes. Every gcloud op is clamped to what
        # remains, and _run_with_timeout hard-kills the process tree on expiry.
        overall_deadline = time.time() + GCS_OP_TIMEOUT + retries * \
            (PUBLISH_TIMEOUT + timeout)

        def remaining() -> float:
            return max(0.0, overall_deadline - time.time())

        uuid_str = str(uuid.uuid4())
        file_name = f"lc_sensor_{uuid_str}.zip"
        uri = f"gs://{self.bucket_name}/{file_name}"
        signed_uri = uri + ".signed"

        try:

            self._upload(input, uri,
                         timeout=min(GCS_OP_TIMEOUT, remaining()))

            req = SigningRequest(sign_type.value, uri, signed_uri)

            message_json = json.dumps(asdict(req))
            message = base64.b64encode(message_json.encode("utf-8"))

            signed = False
            attempt = 0

            for attempt in range(1, retries + 1):

                if remaining() <= 0:
                    break

                try:
                    msg_id = self._publish(
                        message,
                        timeout=min(PUBLISH_TIMEOUT, remaining()))
                except subprocess.TimeoutExpired:
                    logging.warning(f"publish attempt {attempt} timed out")
                    continue

                logging.info(
                    f"attempt {attempt}/{retries}, message id: {msg_id}")

                # wait for the signed file to appear
                end_ts = min(time.time() + timeout, overall_deadline)

                while time.time() < end_ts:

                    op_timeout = min(GCS_OP_TIMEOUT, end_ts - time.time())

                    try:
                        if self._object_exists(signed_uri, timeout=op_timeout):
                            self._download(signed_uri, output,
                                           timeout=min(GCS_OP_TIMEOUT,
                                                       max(1.0, end_ts - time.time())))
                            self._delete(signed_uri,
                                         timeout=min(CLEANUP_TIMEOUT, remaining()))
                            signed = True
                            break
                    except (subprocess.TimeoutExpired, GcloudError) as e:
                        # Race: object disappeared between describe and cp, or
                        # a transient gcloud/network error. Either way keep
                        # polling — the outer retry loop bounds total attempts.
                        logging.warning(f"poll/download/delete failed: {e}")

                    time.sleep(min(5.0, max(0.0, end_ts - time.time())))

                    logging.info(f"timeout={end_ts - time.time():.2f}")

                if signed:
                    break

                logging.warning(
                    f"Signing attempt {attempt}/{retries} timed out")

                sys.stdout.flush()

            if not signed:
                raise TimeoutError(
                    f"Unable to sign {input} after {attempt} attempts "
                    f"(retries={retries})")

        finally:
            # Bound cleanup by whatever wall-clock budget is left (but give
            # it at least 1s so we don't skip it after a deadline blow-out),
            # and never let a cleanup exception mask the original error.
            cleanup_timeout = min(CLEANUP_TIMEOUT, max(1.0, remaining()))
            try:
                self._delete(uri, timeout=cleanup_timeout)
            except Exception:
                logging.warning("bucket cleanup failed for %s",
                                file_name, exc_info=True)

    def sign(self, sign_type: SignFileType, input: str, output: str, timeout: float, retries: int) -> None:

        if input.endswith(".zip"):
            self.sign_archive(sign_type, input, output, timeout, retries)
            return

        with tempfile.TemporaryDirectory(prefix="sign_file_") as td:

            input_fn = os.path.basename(input)

            tmp_bin = os.path.join(td, "bin")
            os.mkdir(tmp_bin)

            unsigned_input = os.path.join(tmp_bin, input_fn)
            shutil.copy2(input, unsigned_input)

            tmp_zip = os.path.join(td, "bin.zip")

            LcUtil.zip(tmp_bin, tmp_zip, include_root=False)

            self.sign_archive(sign_type, tmp_zip, tmp_zip, timeout, retries)

            signed_bin = os.path.join(td, "signed_bin")
            os.mkdir(signed_bin)

            LcUtil.unzip(tmp_zip, signed_bin)

            signed_output = os.path.join(signed_bin, input_fn)

            shutil.copy2(signed_output, output)


def main() -> int:

    status = 1

    parser = argparse.ArgumentParser()

    script_root = os.path.abspath(os.path.dirname(sys.argv[0]))
    config_file = os.path.join(script_root, "config.json")

    config = JSONConfig(config_file)

    def_topic = config.get("/pub/topic")
    def_bucket = config.get("/pub/bucket")
    def_project = config.get("/project")

    if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ:
        default_key = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    else:
        default_key = None

    parser.add_argument("-k",
                        "--key",
                        type=str,
                        default=default_key,
                        help="Google service account key file")

    parser.add_argument("--base64-key",
                        type=str,
                        help="Base64 encoded key")

    parser.add_argument("-v",
                        "--verbose",
                        action="store_true",
                        help="log to stdout")

    parser.add_argument("-i",
                        "--input",
                        type=str,
                        required=True,
                        help="/path/to/lc_sensor.zip")

    parser.add_argument("-o",
                        "--output",
                        type=str,
                        help="/path/to/lc_sensor_signed.zip")

    parser.add_argument("-p",
                        "--project",
                        type=str,
                        default=def_project,
                        help=f"Google sub project. Default: {def_project}")

    parser.add_argument("-t",
                        "--topic",
                        type=str,
                        default=def_topic,
                        help=f"Topic. Default: {def_topic}")

    parser.add_argument("-b",
                        "--bucket",
                        type=str,
                        default=def_bucket,
                        help=f"Bucket name. Default: {def_bucket}")

    parser.add_argument("--timeout",
                        type=float,
                        default=DEF_SIGN_TIMEOUT,
                        help=f"Signing timeout. Default: {DEF_SIGN_TIMEOUT}")

    parser.add_argument("--retries",
                        type=int,
                        default=DEF_SIGN_RETRIES,
                        help=f"Number of signing attempts. Default: {DEF_SIGN_RETRIES}")

    parser.add_argument("--sign-type",
                        type=str,
                        default="sensor",
                        choices=["sensor", "package", "hlk"],
                        help="Type of signing")

    args = parser.parse_args()

    try:
        LcUtil.init_logging("code_signing.log", args.verbose)

        args.input = os.path.abspath(args.input)

        if args.output is None:
            args.output = args.input

        print("Signing Publisher:")
        LcUtil.printkv("Input File", args.input)
        LcUtil.printkv("Input File Size", LcUtil.file_size_fmt(args.input))
        LcUtil.printkv("Input File Hash", LcUtil.md5_file(args.input))
        LcUtil.printkv("Signing Type", args.sign_type)

        if args.key is not None:
            LcUtil.printkv("Google SA Key File", args.key)
        elif args.base64_key is not None:
            LcUtil.printkv("Google SA Key String",
                           args.base64_key[:30] + "...")
        else:
            raise InvalidArgumentError("--key or --base64-key is missing")

        LcUtil.printkv("Project", args.project)
        LcUtil.printkv("Topic", args.topic)
        LcUtil.printkv("Bucket Name", args.bucket)
        LcUtil.printkv("Signing timeout", args.timeout)

        with tempfile.TemporaryDirectory(prefix="pub_client_") as td:

            if args.key is not None:
                key_file = args.key
            else:
                key_file = os.path.join(td, "key.json")
                LcUtil.b64_to_file(args.base64_key, key_file)

            if args.sign_type == "sensor":
                sign_type = SignFileType.SENSOR_ARCHIVE
            elif args.sign_type == "package":
                sign_type = SignFileType.PACKAGE_ARCHIVE
            elif args.sign_type == "hlk":
                sign_type = SignFileType.HLK_ARCHIVE
            else:
                raise NotImplementedError()

            with SigningPublisher(args.project,
                                  args.topic,
                                  args.bucket,
                                  key_file) as pub:

                pub.sign(sign_type, args.input, args.output,
                         args.timeout, args.retries)

        LcUtil.printkv("Output File", args.output)
        LcUtil.printkv("Output File Size", LcUtil.file_size_fmt(args.output))
        LcUtil.printkv("Output File Hash", LcUtil.md5_file(args.output))

        status = 0
    except InvalidArgumentError as e:
        print(e)
    except KeyboardInterrupt:
        pass

    return status


if __name__ == '__main__':

    status = main()

    if 0 != status:
        sys.exit(status)
