# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import logging
from datetime import timedelta

from azure.monitor.query import LogsQueryClient, LogsQueryStatus

from ai4s.jobq.auth import get_sync_token_credential

from ..config import get_workspace_id

credential = get_sync_token_credential()
client = LogsQueryClient(credential)
LOG = logging.getLogger(__name__)


def get_distinct_values(query):
    LOG.debug(f"Fetching distinct values for query: {query}")
    response = client.query_workspace(
        workspace_id=get_workspace_id(),
        query=f"AppTraces | where isnotempty({query}) | summarize LastSeen=arg_max(TimeGenerated, *) by tostring({query}) | sort by LastSeen desc | project {query}",
        timespan=timedelta(days=30),
    )
    if response.status == LogsQueryStatus.SUCCESS:
        LOG.debug(f"Query successful, found {len(response.tables[0].rows)} distinct values.")
        return [row[0] for row in response.tables[0].rows]
    else:
        LOG.debug(f"Query failed with status: {response.status}")
        raise Exception(f"Query failed: {response.status}")


def run_query(query):
    LOG.info(f"Running query: {query}")
    response = client.query_workspace(
        workspace_id=get_workspace_id(),
        query=query,
        timespan=None,
    )
    if response.status == LogsQueryStatus.SUCCESS:
        return response.tables[0].rows
    else:
        raise Exception(f"Query failed: {response.status}")
