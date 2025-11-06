# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import os
import shutil
import typing as ty
import uuid
from datetime import datetime, timedelta
from tempfile import TemporaryDirectory

import amlt
import amlt.api.project
import amlt.azure_io
import amlt.config.core
from amlt.copy_utils import copy_code_dir
from amlt.db.models import BlobArtifactModel
from amlt.helpers.dirchecksum import checksum as directory_checksum
from dateutil.tz import UTC


def upload_code(
    project_client: "amlt.api.project.ProjectClient",
    config: "amlt.config.core.AMLTConfig",
    sas_valid_days: int = 31 * 6,
) -> ty.Optional[str]:
    if config.code is None:
        return None
    if config.code.local_dir is None:
        return None

    with TemporaryDirectory() as tmp_dir:
        local_code_dir = os.path.join(tmp_dir, "code")
        copy_code_dir(config.code.local_dir, local_code_dir, config.code.ignore)
        checksum = directory_checksum(local_code_dir, max_secs=3600)
        assert checksum is not None

        # rename code dir to checksum
        os.rename(local_code_dir, os.path.join(tmp_dir, checksum))
        local_code_dir = os.path.join(tmp_dir, checksum)
        artifact_uuid = str(uuid.uuid4())

        archive_path = project_client.storage.get_path() + f"/amlt-remote-code/{checksum}.zip"

        # remember the artifact we created and schedule its deletion
        with project_client.session.enable_writing():
            project_client.session.add(
                BlobArtifactModel(  # type: ignore
                    project_uuid=project_client.uuid,
                    uuid=artifact_uuid,
                    storage=project_client.storage.storage_config,
                    expires=(datetime.now(UTC) + timedelta(days=sas_valid_days)).timestamp(),
                    path=archive_path,
                )
            )

        filename = shutil.make_archive(
            local_code_dir,
            "zip",
            tmp_dir,
            checksum,
        )
        transport = project_client.storage.transport
        if not transport.exists(archive_path):
            with open(filename, "rb") as f:
                transport._container_service.upload_blob(archive_path, f)

    sas = transport.get_blob_sas(archive_path, timedelta(days=sas_valid_days), read=True)
    url: str = transport.make_blob_url(archive_path, sas=sas)
    return url
