import os
import shutil
import logging
import tempfile
from dataclasses import dataclass

from google.oauth2.service_account import Credentials  # type: ignore
from google.cloud import storage  # type: ignore
from google.api_core.exceptions import NotFound  # type: ignore


@dataclass
class GSIParts:
    bucket_name: str
    object_name: str


class LCGSIFile:

    def __init__(self, key: str) -> None:
        self.creds = Credentials.from_service_account_file(key)  # type: ignore

    def __split_uri(self, gsi: str) -> GSIParts:

        bucket_name = gsi.split("/")[2]
        object_name = "/".join(gsi.split("/")[3:])

        return GSIParts(bucket_name, object_name)

    def delete(self, bucket_name: str, object_name: str) -> None:

        client = storage.Client(credentials=self.creds)  # type: ignore

        logging.info(f"deleting {object_name}")

        bucket = client.bucket(bucket_name)  # type: ignore
        blob = bucket.blob(object_name)  # type: ignore

        try:
            blob.delete()  # type: ignore
        except NotFound:
            pass

    def delete_uri(self, uri: str) -> None:

        parts = self.__split_uri(uri)
        self.delete(parts.bucket_name, parts.object_name)

    def download_uri(self, uri: str, file_path: str) -> None:

        parts = self.__split_uri(uri)
        self.download(parts.bucket_name, parts.object_name, file_path)

    def download(self, bucket_name: str, object_name: str, file_path: str) -> None:

        client = storage.Client(credentials=self.creds)  # type: ignore

        bucket = client.bucket(bucket_name)  # type: ignore
        blob = bucket.blob(object_name)  # type: ignore

        #
        # using a temp file because blob.download_to_filename overwrites
        # the file immediately even when it fails to do so.
        #
        # it was creating 0 byte files :shrug:
        #
        with tempfile.TemporaryDirectory(prefix="gsi_storage_") as td:

            out_file = os.path.join(td, "download.bin")

            logging.info(f"downloading {object_name} to {file_path}")
            blob.download_to_filename(out_file)  # type: ignore

            # it worked. it's not safe to write
            shutil.move(out_file, file_path)

    def upload(self, file_path: str, bucket_name: str, object_name: str) -> str:

        logging.info(f"Uploading {file_path} to {bucket_name}/{object_name}")

        client = storage.Client(credentials=self.creds)  # type: ignore

        bucket = client.bucket(bucket_name)  # type: ignore
        blob = bucket.blob(object_name)  # type: ignore

        blob.upload_from_filename(file_path)  # type: ignore

        return f"gs://{bucket_name}/{object_name}"

    def upload_uri(self, file_path: str, uri: str) -> None:

        parts = self.__split_uri(uri)

        self.upload(file_path, parts.bucket_name, parts.object_name)
