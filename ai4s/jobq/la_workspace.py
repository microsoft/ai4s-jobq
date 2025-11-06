# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import os
import sys
from subprocess import CalledProcessError, check_output

from azure.mgmt.loganalytics import LogAnalyticsManagementClient
from azure.mgmt.resourcegraph import ResourceGraphClient

from ai4s.jobq.auth import get_sync_token_credential
from ai4s.jobq.logging_utils import LOG


class WorkspaceNotFoundError(Exception):
    pass


def workspace_id_from_ikey(ikey: str):
    credential = get_sync_token_credential()
    rg_client = ResourceGraphClient(credential)
    subscription_id = os.environ.get("JOBQ_AZURE_SUBSCRIPTION_ID")
    if not subscription_id:
        try:
            subscription_id = (
                check_output(
                    ["az", "account", "show", "--query", "id", "-o", "tsv"],
                    universal_newlines=True,
                )
                .strip()
                .split("\n")[0]
            )
        except CalledProcessError:
            pass
        LOG.error("You need to specify a subscription id to use instrumentation key lookup.")
        sys.exit(1)

    la_client = LogAnalyticsManagementClient(credential, subscription_id)

    query = f"""
    resources
    | where type == 'microsoft.insights/components'
    | where properties.InstrumentationKey == '{ikey}'
    | project name, resourceGroup, subscriptionId, workspaceResourceId=tostring(properties.WorkspaceResourceId)
    """

    result = rg_client.resources(
        query={"subscriptions": [], "query": query}  # [] = all accessible subs
    )

    if not result.data or len(result.data) == 0:
        raise WorkspaceNotFoundError("No Application Insights found for that instrumentation key.")
    else:
        item = result.data[0]
        LOG.info(
            f"Found App Insights: {item['name']} in RG {item['resourceGroup']} (sub {item['subscriptionId']})"
        )

        if item["workspaceResourceId"]:
            ws_parts = item["workspaceResourceId"].split("/")
            ws_rg = ws_parts[4]
            ws_name = ws_parts[-1]
            sub_id = item["subscriptionId"]

            la_client = LogAnalyticsManagementClient(credential, sub_id)
            ws = la_client.workspaces.get(ws_rg, ws_name)
            LOG.info(f"Workspace name: {ws.name}")
            return ws.customer_id
        else:
            raise WorkspaceNotFoundError("This App Insights isn’t workspace-based.")


def workspace_id_from_connstr(ikey: str):
    # extract ikey from connection string if needed
    if ikey.startswith("InstrumentationKey="):
        ikey = ikey.split("=")[1]
        return workspace_id_from_ikey(ikey)


def workspace_id_from_workspace_resource_id(rid: str):
    if rid.startswith("/subscriptions/"):
        parts = rid.split("/")
        la_client = LogAnalyticsManagementClient(
            get_sync_token_credential(),
            parts[2],
        )
        ws = la_client.workspaces.get(parts[4], parts[-1])
        LOG.info(f"Workspace name: {ws.name}")
        return ws.customer_id


def is_uid(s: str) -> bool:
    import re

    pattern = re.compile(
        r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    )
    return bool(pattern.match(s))


def workspace_id_from_ws_name(name: str):
    LOG.info("Assuming Application Insights workspace name...")
    credential = get_sync_token_credential()
    rg_client = ResourceGraphClient(credential)

    query = f"""
    resources
    | where type == 'microsoft.operationalinsights/workspaces'
    | where name == '{name}'
    | project name, resourceGroup, subscriptionId, customerId=tostring(properties.customerId)
    """

    result = rg_client.resources(
        query={"subscriptions": [], "query": query}  # [] = all accessible subs
    )

    if not result.data or len(result.data) == 0:
        raise WorkspaceNotFoundError("No Log Analytics workspace found with that name.")
    else:
        item = result.data[0]
        LOG.info(
            f"Found LA Workspace: {item['name']} in RG {item['resourceGroup']} (sub {item['subscriptionId']})"
        )
        return item["customerId"]
