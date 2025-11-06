# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import asyncio
import os
import re
import shutil
import typing as ty
from pathlib import Path
from subprocess import call
from tempfile import TemporaryDirectory

import aiohttp

try:
    from typing import Self  # type: ignore
except ImportError:
    from typing_extensions import Self

from ai4s.jobq.work import Processor, ShellCommandProcessor

from ..work import _AbstractAsyncContextManager
from .background_dirsync import BackgroundDirSync

if ty.TYPE_CHECKING:
    pass


AMULET_REMOTE_LOCAL_DIR_TEMPLATE = "/mnt/default/amulet-remote/{uuid}"


def generate_sas_token(file_name):
    from datetime import datetime, timedelta

    from azure.storage.blob import BlobSasPermissions, generate_blob_sas

    account_name = "devstoreaccount1"
    account_key = (
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
    )
    container_name = "mock0container"

    sas = generate_blob_sas(
        account_name=account_name,
        account_key=account_key,
        container_name=container_name,
        blob_name=file_name,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=2),
    )
    return sas


class MapInConfigProcessor(Processor):
    def __init__(self, num_workers: int = 1) -> None:
        super().__init__()

        self.shell_cmd_proc = ShellCommandProcessor(num_workers)
        self.code_cache = CodeCache()
        self.register_context_manager(self.shell_cmd_proc, self.code_cache)
        self.main_cmd = "python -m utilities.amlt_remote_main"

    @staticmethod
    def _truish(value: ty.Optional[str]) -> bool:
        if value is None:
            return False
        return value.lower() in ("true", "1", "yes", "y")

    @classmethod
    def _run(cls, cmd: str, env: ty.Dict[str, str]) -> int:
        with BackgroundDirSync(
            src=env["AMLT_DIRSYNC_DIR"],
            dst=env["AMLT_OUTPUT_DIR"],
            delete_after_copy=cls._truish(env.get("AMLT_DELETE_AFTER_COPY")),
            freq=int(env.get("AMLT_DIRSYNC_FREQ", 30)),
            n_threads=int(env.get("AMLT_DIRSYNC_N_THREADS", 5)),
            include=env.get("AMLT_DIRSYNC_INCLUDE"),
            exclude=env.get("AMLT_DIRSYNC_INCLUDE"),
            remove_if_not_in_source=False,
        ):
            return call(cmd, shell=True, env=env, cwd=env.get("AMLT_CODE_DIR", "."))

    async def __call__(
        self, uuid: str, _job_id: str, code_zip_url: ty.Optional[str] = None
    ) -> None:
        cmd = f"{self.main_cmd} {uuid}"

        env = os.environ.copy()
        env.update(
            dict(
                PYTHONPATH="projects",
                AMULET_REMOTE_LOCAL_DIR_TEMPLATE=AMULET_REMOTE_LOCAL_DIR_TEMPLATE,
            )
        )
        cwd: ty.Optional[Path] = Path(".")
        if code_zip_url is not None:
            cwd = await self.code_cache.get_local_code_dir(code_zip_url)

        await self.shell_cmd_proc(
            cmd=cmd,
            env=env,
            cwd=str(cwd),
            _job_id=_job_id,
            bg_dirsync_to=f"/mnt/default/amulet-remote/{uuid}",
        )


class CodeCache(_AbstractAsyncContextManager["CodeCache"]):
    LOCK = asyncio.Lock()
    CACHE: ty.Dict[str, Path] = {}

    def __init__(self, session: ty.Optional[aiohttp.ClientSession] = None) -> None:
        super().__init__()
        self.tempdir: ty.Optional[TemporaryDirectory[str]] = None
        self.session = session or aiohttp.ClientSession()

    async def __aenter__(self) -> Self:
        await super().__aenter__()
        self.tempdir = TemporaryDirectory()
        return self

    async def __aexit__(self, *args: ty.Any) -> None:
        if self.tempdir is not None:
            self.tempdir.cleanup()
        await super().__aexit__(*args)
        return None

    async def get_local_code_dir(
        self, url: str, headers: ty.Optional[ty.Dict[str, str]] = None
    ) -> ty.Optional[Path]:
        if url.startswith("https://devstoreaccount1.blob.core.windows.net"):
            # this is for azurite compatibility
            url = url.replace(
                "https://devstoreaccount1.blob.core.windows.net",
                "http://127.0.0.1:10000",
            )
            headers = headers or {}
            headers.update(dict(host="devstoreaccount1.blob.core.windows.net"))

            # add the azurite default sas token
            if "?sv" not in url:
                file_name = "/".join(url.split("/")[-4:])
                url += "?" + generate_sas_token(file_name)

        assert self.tempdir is not None, "Not yet initialized, use in async-with context."
        filename = re.match(r".*?/(\w+\.zip)", url).group(1)  # type: ignore
        async with self.LOCK:
            if url not in self.CACHE:
                resp = await self.session.get(url, headers=headers)
                if resp.status != 200:
                    raise RuntimeError(f"Code not found at {url!r}.")
                full_filename = os.path.join(self.tempdir.name, filename)
                with open(full_filename, "wb") as f:
                    async for chunk in resp.content.iter_any():
                        f.write(chunk)
                shutil.unpack_archive(full_filename, self.tempdir.name)
                self.CACHE[url] = Path(full_filename.rsplit(".", 1)[0])

        return self.CACHE.get(url)
