from contextlib import asynccontextmanager
from unittest.mock import patch

import pytest

from ai4s.jobq.orchestration.multiregion_workforce import MultiRegionWorkforce


@asynccontextmanager
async def mock_from_storage_queue(name: str, *, storage_account: str, credential, exist_ok=True):
    yield storage_account


@asynccontextmanager
async def mock_from_service_bus(name: str, *, fqns: str, credential, exist_ok=True):
    yield fqns


@pytest.mark.parametrize("storage_account", ["storage_account", "sb://servicebus"])
@patch("ai4s.jobq.orchestration.multiregion_workforce.JobQ")
@pytest.mark.asyncio
async def test_get_jobq(MockJobQ, storage_account):
    MockJobQ.from_storage_queue = mock_from_storage_queue
    MockJobQ.from_service_bus = mock_from_service_bus

    mrw = MultiRegionWorkforce(
        queue_name="queue_name", storage_account=storage_account, workforces=[]
    )
    async with mrw.get_jobq() as jobq:
        if storage_account == "storage_account":
            assert jobq == storage_account
        else:
            assert jobq == f'{storage_account.rsplit("/", maxsplit=1)[1]}.servicebus.windows.net'
