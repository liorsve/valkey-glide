# Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

import random
from typing import AsyncGenerator, Generator, List, Optional, Union

import pytest
from glide.config import (
    AdvancedGlideClientConfiguration,
    AdvancedGlideClusterClientConfiguration,
    BackoffStrategy,
    GlideClientConfiguration,
    GlideClusterClientConfiguration,
    NodeAddress,
    ProtocolVersion,
    ReadFrom,
    ServerCredentials,
)
from glide.exceptions import ClosingError
from glide.glide_client import GlideClient, GlideClusterClient, TGlideClient
from glide.logger import Level as logLevel
from glide.logger import Logger
from glide.routes import AllNodes
from glide.sync import GlideClient as SyncGlideClient
from glide.sync import GlideClusterClient as SyncGlideClusterClient
from glide.sync import TGlideClient as TSyncGlideClient
from tests.conftest import DEFAULT_TEST_LOG_LEVEL, NEW_PASSWORD, create_client_config
from tests.utils.cluster import ValkeyCluster
from tests.utils.utils import (
    check_if_server_version_lt,
    set_new_acl_username_with_password,
    sync_check_if_server_version_lt,
)
Logger.set_logger_config(DEFAULT_TEST_LOG_LEVEL)

@pytest.fixture(scope="function")
def glide_sync_client(
    request,
    cluster_mode: bool,
    protocol: ProtocolVersion,
) -> Generator[TSyncGlideClient, None, None]:
    "Get async socket client for tests"
    client = create_sync_client(request, cluster_mode, protocol=protocol)
    yield client
    sync_test_teardown(request, cluster_mode, protocol)
    client.close()

def create_sync_client(
    request,
    cluster_mode: bool,
    credentials: Optional[ServerCredentials] = None,
    database_id: int = 0,
    addresses: Optional[List[NodeAddress]] = None,
    client_name: Optional[str] = None,
    protocol: ProtocolVersion = ProtocolVersion.RESP3,
    timeout: Optional[int] = 1000,
    connection_timeout: Optional[int] = 1000,
    cluster_mode_pubsub: Optional[
        GlideClusterClientConfiguration.PubSubSubscriptions
    ] = None,
    standalone_mode_pubsub: Optional[
        GlideClientConfiguration.PubSubSubscriptions
    ] = None,
    inflight_requests_limit: Optional[int] = None,
    read_from: ReadFrom = ReadFrom.PRIMARY,
    client_az: Optional[str] = None,
    valkey_cluster: Optional[ValkeyCluster] = None,
) -> TSyncGlideClient:
    # Create sync client
    config = create_client_config(
        request,
        cluster_mode,
        credentials,
        database_id,
        addresses,
        client_name,
        protocol,
        timeout,
        connection_timeout,
        cluster_mode_pubsub,
        standalone_mode_pubsub,
        inflight_requests_limit,
        read_from,
        client_az,
        valkey_cluster,
    )
    if cluster_mode:
        return SyncGlideClusterClient.create(config)
    else:
        return SyncGlideClient.create(config)
    

def sync_auth_client(client: TSyncGlideClient, password):
    """
    Authenticates the given TGlideClient server connected.
    """
    if isinstance(client, GlideClient):
        client.custom_command(["AUTH", password])
    elif isinstance(client, GlideClusterClient):
        client.custom_command(["AUTH", password], route=AllNodes())


def sync_config_set_new_password(client: TSyncGlideClient, password):
    """
    Sets a new password for the given TGlideClient server connected.
    This function updates the server to require a new password.
    """
    if isinstance(client, GlideClient):
        client.config_set({"requirepass": password})
    elif isinstance(client, GlideClusterClient):
        client.config_set({"requirepass": password}, route=AllNodes())


def sync_kill_connections(client: TSyncGlideClient):
    """
    Kills all connections to the given TGlideClient server connected.
    """
    if isinstance(client, GlideClient):
        client.custom_command(["CLIENT", "KILL", "TYPE", "normal"])
    elif isinstance(client, GlideClusterClient):
        client.custom_command(["CLIENT", "KILL", "TYPE", "normal"], route=AllNodes())



def sync_test_teardown(request, cluster_mode: bool, protocol: ProtocolVersion):
    """
    Perform teardown tasks such as flushing all data from the cluster.

    If authentication is required, attempt to connect with the known password,
    reset it back to empty, and proceed with teardown.
    """
    credentials = None
    try:
        # Try connecting without credentials
        client = create_sync_client(
            request, cluster_mode, protocol=protocol, timeout=2000
        )
        client.custom_command(["FLUSHALL"])
        client.close()
    except ClosingError as e:
        # Check if the error is due to authentication
        if "NOAUTH" in str(e):
            # Use the known password to authenticate
            credentials = ServerCredentials(password=NEW_PASSWORD)
            client = create_sync_client(
                request,
                cluster_mode,
                protocol=protocol,
                timeout=2000,
                credentials=credentials,
            )
            try:
                sync_auth_client(client, NEW_PASSWORD)
                # Reset the server password back to empty
                sync_config_set_new_password(client, "")
                client.update_connection_password(None)
                # Perform the teardown
                client.custom_command(["FLUSHALL"])
            finally:
                client.close()
        else:
            raise e
        
@pytest.fixture(autouse=True)
def skip_if_version_below(request):
    """
    Skip test(s) if server version is below than given parameter. Can skip a complete test suite.

    Example:
        @pytest.mark.skip_if_version_below('7.0.0')
        async def test_meow_meow(...):
            ...
    """
    if request.node.get_closest_marker("skip_if_version_below"): 
        min_version = request.node.get_closest_marker("skip_if_version_below").args[0]
        client = create_sync_client(request, False)
        if sync_check_if_server_version_lt(client, min_version):
            pytest.skip(
                reason=f"This feature added in version {min_version}",
                allow_module_level=True,
            )
