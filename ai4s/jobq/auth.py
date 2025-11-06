# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import os
from functools import lru_cache
from logging import getLogger

from azure.core.credentials import TokenCredential
from azure.core.credentials_async import AsyncTokenCredential

LOG = getLogger(__name__)


@lru_cache(2)
def get_token_credential() -> AsyncTokenCredential:
    """
    Make a good guess which token credential to use.

    We avoid using DefaultAzureCredential, since on sandboxes, this picks up the managed identity.
    We check whether DEFAULT_IDENTITY_CLIENT_ID is set, in which case this is
    likely a user-assigned managed identity, eg of an aml cluster.

    Finally, we return the AzureCliCredential.
    """
    from azure.identity.aio import (
        AzureCliCredential,
        ManagedIdentityCredential,
    )

    if "DEFAULT_IDENTITY_CLIENT_ID" in os.environ:
        LOG.info("Authenticating with ManagedIdentityCredential()")
        return ManagedIdentityCredential(client_id=os.environ["DEFAULT_IDENTITY_CLIENT_ID"])  # type: ignore
    else:
        LOG.info("Authenticating with AzureCliCredential()")
        return AzureCliCredential()  # type: ignore


@lru_cache(2)
def get_sync_token_credential() -> TokenCredential:
    """
    Make a good guess which token credential to use.

    We avoid using DefaultAzureCredential, since on sandboxes, this picks up the managed identity.
    We check whether DEFAULT_IDENTITY_CLIENT_ID is set, in which case this is
    likely a user-assigned managed identity, eg of an aml cluster.

    Finally, we return the AzureCliCredential.
    """
    from azure.identity import (
        AzureCliCredential,
        ManagedIdentityCredential,
    )

    if "DEFAULT_IDENTITY_CLIENT_ID" in os.environ:
        LOG.info("Authenticating with ManagedIdentityCredential()")
        return ManagedIdentityCredential(client_id=os.environ["DEFAULT_IDENTITY_CLIENT_ID"])  # type: ignore
    else:
        LOG.info("Authenticating with AzureCliCredential()")
        return AzureCliCredential()  # type: ignore
