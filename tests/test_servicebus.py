import pytest
from azure.identity.aio import DefaultAzureCredential

from ai4s.jobq import JobQ

TEST_SB = "ai4s-shared/testq"


@pytest.mark.live
@pytest.mark.asyncio
async def test_servicebus():
    async def callback(cmd):
        return f"processed {cmd}"

    async with JobQ.from_service_bus(
        "testq",
        fqns="ai4s-shared.servicebus.windows.net",
        credential=DefaultAzureCredential(),
    ) as jobq:
        await jobq.clear()
        fut = await jobq.push("test", reply_requested=True)
        await jobq.pull_and_execute(callback)
        result = await fut
        assert result.body == "processed test"
