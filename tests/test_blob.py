import os
from pathlib import Path

import pytest
from azure.storage.blob import BlobProperties
from azure.storage.blob.aio import BlobServiceClient

from ai4s.jobq.blob import BlobContainer


@pytest.mark.asyncio
async def test_blob_processor(mocker, tmp_path) -> None:
    container = "mock0container"
    blob_port = os.environ.get("BLOB_PORT", 10000)
    bsc = BlobServiceClient.from_connection_string(
        f"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:{blob_port}/devstoreaccount1;"
    )
    try:
        await bsc.create_container(container)
    except Exception:
        pass
    mocker.patch("ai4s.jobq.blob.BlobServiceClient", return_value=bsc)
    async with BlobContainer(
        storage_account="mock0storage0account",
        container=container,
        credential="mock0credential",
    ) as bp:
        local_path = tmp_path.joinpath("mock0blob")
        local_path.write_text("mock0content")
        await bp.upload_file(local_path, "remote_path")
        assert await bsc.get_blob_client(container, "remote_path").exists()
        assert await bp.blob_exists("remote_path")
        downloaded_path = await bp.download_file("remote_path", tmp_path)
        assert downloaded_path.startswith(str(tmp_path))
        assert Path(downloaded_path).read_text() == "mock0content"

        downloaded_path2, properties = await bp.download_file_with_properties(
            "remote_path", tmp_path
        )
        assert Path(downloaded_path2).read_text() == Path(downloaded_path).read_text()
        assert isinstance(properties, BlobProperties)
        # check a random property
        assert properties.last_modified is not None

        subfolder = tmp_path.joinpath("subfolder")
        subfolder.mkdir(parents=True, exist_ok=True)
        local_path1 = subfolder.joinpath("mock1file")
        local_path2 = subfolder.joinpath("mock2file")
        local_path1.write_text("mock1content")
        local_path2.write_text("mock2content")
        await bp.upload_from_folder(subfolder, "remote_path")
        assert await bsc.get_blob_client(container, "remote_path/mock1file").exists()
        assert await bsc.get_blob_client(container, "remote_path/mock2file").exists()


@pytest.mark.asyncio
async def test_blob_stash(mocker) -> None:
    container = "mock0container"
    blob_port = os.environ.get("BLOB_PORT", 10000)
    bsc = BlobServiceClient.from_connection_string(
        f"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:{blob_port}/devstoreaccount1;"
    )
    try:
        await bsc.create_container(container)
    except Exception:
        pass
    mocker.patch("ai4s.jobq.blob.BlobServiceClient", return_value=bsc)
    async with BlobContainer(
        storage_account="mock0storage0account",
        container=container,
        credential="mock0credential",
    ) as bp:
        data = "mock0data"
        blob_stash = await bp.stash_as_pickle(data)
        assert await bp.blob_exists(blob_stash.filename)
        assert await bp.unstash_from_pickle(blob_stash.filename, blob_stash.md5sum) == data
