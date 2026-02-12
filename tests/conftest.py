import os

import pytest

from ai4s.jobq import JobQ

BLOB_PORT = os.environ.get("BLOB_PORT", 10000)
QUEUE_PORT = os.environ.get("QUEUE_PORT", 10001)


def pytest_addoption(parser):
    parser.addoption(
        "--run-live",
        action="store_true",
        default=False,
        help="run live servicebus tests",
    )
    parser.addoption(
        "--run-stress-tests",
        action="store_true",
        default=False,
        help="run stress tests (e.g. lock renewal under load)",
    )
    parser.addoption(
        "--sb-namespace", action="store", default="ai4s-shared", help="Azure Service Bus namespace"
    )
    parser.addoption(
        "--sb-queue", action="store", default="testq", help="Azure Service Bus queue name"
    )


@pytest.fixture
def sb_namespace(request):
    return request.config.getoption("--sb-namespace")


@pytest.fixture
def sb_queue(request):
    return request.config.getoption("--sb-queue")


def pytest_configure(config):
    config.addinivalue_line("markers", "live: mark test as live run that accesses azure resources")
    config.addinivalue_line(
        "markers", "stress: mark test as stress test (slow, resource-intensive)"
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--run-live"):
        skip_live = pytest.mark.skip(reason="need --run-live option to run")
        for item in items:
            if "live" in item.keywords:
                item.add_marker(skip_live)
    if not config.getoption("--run-stress-tests"):
        skip_stress = pytest.mark.skip(reason="need --run-stress-tests option to run")
        for item in items:
            if "stress" in item.keywords:
                item.add_marker(skip_stress)


CONNSTR = (
    f"DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
    f"QueueEndpoint=http://127.0.0.1:{QUEUE_PORT}/devstoreaccount1;"
)


@pytest.fixture
async def azurite_connstr():
    return CONNSTR


@pytest.fixture
async def async_queue(azurite_connstr):
    async with JobQ.from_connection_string("jobs", connection_string=azurite_connstr) as q:
        await q.clear()
        yield q
