# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import hashlib
import pickle
import uuid
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any

from monty.json import MSONable
from utilities.blob_storage import BlobClient
from utilities.common import StrPath


@dataclass(frozen=True)
class BlobStasher:
    """Stores cache of some data in blob storage.

    Args:
        blob_storage_uri: URI formatted as blob-storage://{account name}/{container name}/path/to/directory.
    """

    blob_storage_uri: str

    @cached_property
    def blob_client(self) -> BlobClient:
        return BlobClient.from_blob_storage_uri(self.blob_storage_uri)

    def store(self, data: Any, filename: str | None = None) -> "BlobStash":
        """Stores some data in blob storage.

        Args:
            data: any picklable object.
            filename: (optional) if provided, stores the cache under the specified filename. Otherwise, a random
                filename is generated.
        """
        if filename is None:
            filename = f"{uuid.uuid4()}.pck"
        content = pickle.dumps(data)
        md5sum = hashlib.md5(content).hexdigest()
        with self.blob_client:
            local_file_path = Path(self.blob_client.local_dir) / filename
            with open(local_file_path, "wb") as f:
                f.write(content)
            self.blob_client.upload_file(local_file_path)
        return BlobStash(
            blob_storage_uri=self.blob_storage_uri,
            filename=filename,
            md5sum=md5sum,
        )


@dataclass(frozen=True)
class BlobStash(MSONable):
    """Object representing a pickled object stored in blob storage.

    Args:
        blob_storage_uri: URI formatted as blob-storage://{account name}/{container name}/path/to/directory.
        filename: name of the pickled file.
        md5sum: (optional) md5sum of the pickled file.
        local_cache_dir: (optional) if provided, saves the cache also locally under the specified directory.
    """

    blob_storage_uri: str
    filename: str
    md5sum: str | None = None
    local_cache_dir: StrPath | None = None

    @cached_property
    def blob_client(
        self,
    ) -> BlobClient:
        return BlobClient.from_blob_storage_uri(
            self.blob_storage_uri,
            local_dir=self.local_cache_dir if self.local_cache_dir else ".",
        )

    def possibly_temporary_directory_for_blob_client(
        self,
    ) -> AbstractContextManager[None]:
        # Using the BlobClient as a context manager downloads the file to a temporary directory.
        return nullcontext() if self.local_cache_dir else self.blob_client  # type: ignore

    def retrieve(self) -> Any:
        expected_md5sum = self.md5sum
        with self.possibly_temporary_directory_for_blob_client():
            self.blob_client.download_file(self.filename)
            with open(Path(self.blob_client.local_dir) / self.filename, "rb") as f:
                content = f.read()
                md5sum = hashlib.md5(content).hexdigest()
                if expected_md5sum and md5sum != expected_md5sum:
                    raise RuntimeError(
                        f"The md5 hash ({md5sum}) of {self.blob_storage_uri}/{self.filename} does not match the expected md5 hash ({expected_md5sum})!"
                    )
                return pickle.loads(content)
