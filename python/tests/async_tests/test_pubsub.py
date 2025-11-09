# Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

from __future__ import annotations

from enum import IntEnum
from typing import Any, Dict, List, Optional, Set, Tuple, Union, cast

import anyio
import pytest
from glide.glide_client import GlideClient, GlideClusterClient, TGlideClient
from glide_shared.commands.core_options import PubSubMsg
from glide_shared.config import (
    GlideClientConfiguration,
    GlideClusterClientConfiguration,
    ProtocolVersion,
)
from glide_shared.constants import OK
from glide_shared.exceptions import ConfigurationError
from glide.async_commands.mock_pubsub import MockPubSubBroker

from tests.async_tests.conftest import create_client
from tests.utils.utils import (
    check_if_server_version_lt,
    create_pubsub_subscription,
    decode_pubsub_msg,
    get_random_string,
    new_message,
    wait_for_subscription_state,
)


class MethodTesting(IntEnum):
    """
    Enumeration for specifying the method of PUBSUB subscription.
    """

    Async = 0
    "Uses asynchronous subscription method."
    Sync = 1
    "Uses synchronous subscription method."
    Callback = 2
    "Uses callback-based subscription method."


async def create_two_clients_with_pubsub(
    request,
    cluster_mode,
    client1_pubsub: Optional[Any] = None,
    client2_pubsub: Optional[Any] = None,
    protocol: ProtocolVersion = ProtocolVersion.RESP3,
    timeout: Optional[int] = None,
) -> Tuple[TGlideClient, TGlideClient]:
    """
    Sets 2 up clients for testing purposes with optional pubsub configuration.

    Args:
        request: pytest request for creating a client.
        cluster_mode: the cluster mode.
        client1_pubsub: pubsub configuration subscription for the first client.
        client2_pubsub: pubsub configuration subscription for the second client.
        protocol: what protocol to use, used for the test: `test_pubsub_resp2_raise_an_error`.
    """
    cluster_mode_pubsub1, standalone_mode_pubsub1 = None, None
    cluster_mode_pubsub2, standalone_mode_pubsub2 = None, None
    if cluster_mode:
        cluster_mode_pubsub1 = client1_pubsub
        cluster_mode_pubsub2 = client2_pubsub
    else:
        standalone_mode_pubsub1 = client1_pubsub
        standalone_mode_pubsub2 = client2_pubsub

    client1 = await create_client(
        request,
        cluster_mode=cluster_mode,
        cluster_mode_pubsub=cluster_mode_pubsub1,
        standalone_mode_pubsub=standalone_mode_pubsub1,
        protocol=protocol,
        request_timeout=timeout,
    )
    try:
        client2 = await create_client(
            request,
            cluster_mode=cluster_mode,
            cluster_mode_pubsub=cluster_mode_pubsub2,
            standalone_mode_pubsub=standalone_mode_pubsub2,
            protocol=protocol,
            request_timeout=timeout,
        )
    except Exception as e:
        await client1.close()
        raise e

    return client1, client2


async def get_message_by_method(
    method: MethodTesting,
    client: TGlideClient,
    messages: Optional[List[PubSubMsg]] = None,
    index: Optional[int] = None,
):
    if method == MethodTesting.Async:
        return decode_pubsub_msg(await client.get_pubsub_message())
    elif method == MethodTesting.Sync:
        return decode_pubsub_msg(client.try_get_pubsub_message())
    assert messages and (index is not None)
    return decode_pubsub_msg(messages[index])


async def check_no_messages_left(
    method,
    client: TGlideClient,
    callback: Optional[List[Any]] = None,
    expected_callback_messages_count: int = 0,
):
    if method == MethodTesting.Async:
        # assert there are no messages to read
        with pytest.raises(TimeoutError):
            with anyio.fail_after(3):
                await client.get_pubsub_message()
    elif method == MethodTesting.Sync:
        assert client.try_get_pubsub_message() is None
    else:
        assert callback is not None
        assert len(callback) == expected_callback_messages_count


async def client_cleanup(
    client: Optional[Union[GlideClient, GlideClusterClient]],
):
    """
    This function tries its best to clear state assosiated with client
    Its explicitly calls client.close() and deletes the object
    In addition, it tries to clean up cluster mode subsciptions since it was found the closing the client via close() is
    not enough.
    Note that unsubscribing is not feasible in the current implementation since its unknown on which node the subs
    are configured
    """

    if client is None:
        return

    cleanup_error = None

    try:
        active = await client.get_active_subscriptions()
        has_channels = len(active.get("channels", set())) > 0
        has_patterns = len(active.get("patterns", set())) > 0
        has_sharded = len(active.get("sharded_channels", set())) > 0

        if has_channels:
            await client.unsubscribe()
        if has_patterns:
            await client.punsubscribe()
        if has_sharded and isinstance(client, GlideClusterClient):
            await client.sunsubscribe()

        if has_channels or has_patterns or has_sharded:
            await wait_for_subscription_state(
                client,
                expected_channels=set(),
                expected_patterns=set(),
                expected_sharded=set(),
                timeout=5.0,
            )

    except Exception as e:
        # We catch the error so that we can close the client, then re-raise it
        cleanup_error = e
    finally:
        await client.close()
        del client
        # The closure is not completed in the glide-core instantly
        await anyio.sleep(1)

        # Re-raise cleanup error if it occurred
        if cleanup_error:
            raise cleanup_error


@pytest.mark.anyio
class TestPubSub:
    @pytest.fixture(autouse=True)
    async def reset_broker(self):
        """Reset broker before each test in this class"""
        # TODO: remove when mock pubsub is removed
        yield
        MockPubSubBroker.reset()
        
    @pytest.mark.parametrize("cluster_mode", [True, False])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_pubsub_exact_happy_path(
        self,
        request,
        cluster_mode: bool,
        method: MethodTesting,
    ):
        """
        Tests the basic happy path for exact PUBSUB functionality.

        This test covers the basic PUBSUB flow using three different methods:
        Async, Sync, and Callback. It verifies that a message published to a
        specific channel is correctly received by a subscriber.
        """
        listening_client, publishing_client = None, None
        try:
            channel = get_random_string(10)
            message = get_random_string(5)

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {GlideClusterClientConfiguration.PubSubChannelModes.Exact: {channel}},
                {GlideClientConfiguration.PubSubChannelModes.Exact: {channel}},
                callback=callback,
                context=context,
            )

            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub
            )

            result = await publishing_client.publish(message, channel)
            if cluster_mode:
                assert result == 1
            # allow the message to propagate
            await anyio.sleep(1)

            pubsub_msg = await get_message_by_method(
                method, listening_client, callback_messages, 0
            )

            assert pubsub_msg.message == message
            assert pubsub_msg.channel == channel
            assert pubsub_msg.pattern is None

            await check_no_messages_left(method, listening_client, callback_messages, 1)
        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    async def test_pubsub_exact_happy_path_coexistence(
        self, request, cluster_mode: bool
    ):
        """
        Tests the coexistence of async and sync message retrieval methods in exact PUBSUB.

        This test covers the scenario where messages are published to a channel
        and received using both async and sync methods to ensure that both methods
        can coexist and function correctly.
        """
        listening_client, publishing_client = None, None
        try:
            channel = get_random_string(10)
            message = get_random_string(5)
            message2 = get_random_string(7)

            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {GlideClusterClientConfiguration.PubSubChannelModes.Exact: {channel}},
                {GlideClientConfiguration.PubSubChannelModes.Exact: {channel}},
            )

            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub
            )

            for msg in [message, message2]:
                result = await publishing_client.publish(msg, channel)
                if cluster_mode:
                    assert result == 1

            # allow the message to propagate
            await anyio.sleep(1)

            async_msg_res = await listening_client.get_pubsub_message()
            sync_msg_res = listening_client.try_get_pubsub_message()
            assert sync_msg_res
            async_msg = decode_pubsub_msg(async_msg_res)
            sync_msg = decode_pubsub_msg(sync_msg_res)

            assert async_msg.message in [message, message2]
            assert async_msg.channel == channel
            assert async_msg.pattern is None

            assert sync_msg.message in [message, message2]
            assert sync_msg.channel == channel
            assert sync_msg.pattern is None
            # we do not check the order of the messages, but we can check that we received both messages once
            assert not sync_msg.message == async_msg.message

            # assert there are no messages to read
            with pytest.raises(TimeoutError):
                with anyio.fail_after(3):
                    await listening_client.get_pubsub_message()

            assert listening_client.try_get_pubsub_message() is None
        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_pubsub_exact_happy_path_many_channels(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Tests publishing and receiving messages across many channels in exact PUBSUB.

        This test covers the scenario where multiple channels each receive their own
        unique message. It verifies that messages are correctly published and received
        using different retrieval methods: async, sync, and callback.
        """
        listening_client, publishing_client = None, None
        try:
            NUM_CHANNELS = 256
            shard_prefix = "{same-shard}"

            # Create a map of channels to random messages with shard prefix
            channels_and_messages = {
                f"{shard_prefix}{get_random_string(10)}": get_random_string(5)
                for _ in range(NUM_CHANNELS)
            }

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Exact: set(
                        channels_and_messages.keys()
                    )
                },
                {
                    GlideClientConfiguration.PubSubChannelModes.Exact: set(
                        channels_and_messages.keys()
                    )
                },
                callback=callback,
                context=context,
            )
            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub
            )

            # Publish messages to each channel
            for channel, message in channels_and_messages.items():
                result = await publishing_client.publish(message, channel)
                if cluster_mode:
                    assert result == 1

            # Allow the messages to propagate
            await anyio.sleep(1)

            # Check if all messages are received correctly
            for index in range(len(channels_and_messages)):
                pubsub_msg = await get_message_by_method(
                    method, listening_client, callback_messages, index
                )
                assert pubsub_msg.channel in channels_and_messages.keys()
                assert pubsub_msg.message == channels_and_messages[pubsub_msg.channel]
                assert pubsub_msg.pattern is None
                del channels_and_messages[pubsub_msg.channel]

            # check that we received all messages
            assert channels_and_messages == {}
            # check no messages left
            await check_no_messages_left(
                method, listening_client, callback_messages, NUM_CHANNELS
            )

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    async def test_pubsub_exact_happy_path_many_channels_co_existence(
        self, request, cluster_mode: bool
    ):
        """
        Tests publishing and receiving messages across many channels in exact PUBSUB, ensuring coexistence of async and sync
        retrieval methods.

        This test covers scenarios where multiple channels each receive their own unique message.
        It verifies that messages are correctly published and received using both async and sync methods to ensure that
        both methods
        can coexist and function correctly.
        """
        listening_client, publishing_client = None, None
        try:
            NUM_CHANNELS = 256
            shard_prefix = "{same-shard}"

            # Create a map of channels to random messages with shard prefix
            channels_and_messages = {
                f"{shard_prefix}{get_random_string(10)}": get_random_string(5)
                for _ in range(NUM_CHANNELS)
            }

            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Exact: set(
                        channels_and_messages.keys()
                    )
                },
                {
                    GlideClientConfiguration.PubSubChannelModes.Exact: set(
                        channels_and_messages.keys()
                    )
                },
            )

            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub
            )

            # Publish messages to each channel
            for channel, message in channels_and_messages.items():
                result = await publishing_client.publish(message, channel)
                if cluster_mode:
                    assert result == 1

            # Allow the messages to propagate
            await anyio.sleep(1)

            # Check if all messages are received correctly by each method
            for index in range(len(channels_and_messages)):
                method = MethodTesting.Async if index % 2 else MethodTesting.Sync
                pubsub_msg = await get_message_by_method(method, listening_client)

                assert pubsub_msg.channel in channels_and_messages.keys()
                assert pubsub_msg.message == channels_and_messages[pubsub_msg.channel]
                assert pubsub_msg.pattern is None
                del channels_and_messages[pubsub_msg.channel]

            # check that we received all messages
            assert channels_and_messages == {}
            # assert there are no messages to read
            with pytest.raises(TimeoutError):
                with anyio.fail_after(3):
                    await listening_client.get_pubsub_message()

            assert listening_client.try_get_pubsub_message() is None

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.skip_if_version_below("7.0.0")
    @pytest.mark.parametrize("cluster_mode", [True])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_sharded_pubsub(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Test sharded PUBSUB functionality with different message retrieval methods.

        This test covers the sharded PUBSUB flow using three different methods:
        Async, Sync, and Callback. It verifies that a message published to a
        specific sharded channel is correctly received by a subscriber.
        """
        listening_client, publishing_client = None, None
        try:
            channel = get_random_string(10)
            message = get_random_string(5)
            publish_response = 1

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {GlideClusterClientConfiguration.PubSubChannelModes.Sharded: {channel}},
                {},
                callback=callback,
                context=context,
            )

            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub
            )

            assert (
                await cast(GlideClusterClient, publishing_client).publish(
                    message, channel, sharded=True
                )
                == publish_response
            )

            # allow the message to propagate
            await anyio.sleep(1)

            pubsub_msg = await get_message_by_method(
                method, listening_client, callback_messages, 0
            )
            assert pubsub_msg.message == message
            assert pubsub_msg.channel == channel
            assert pubsub_msg.pattern is None

            # assert there are no messages to read
            await check_no_messages_left(method, listening_client, callback_messages, 1)

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.skip_if_version_below("7.0.0")
    @pytest.mark.parametrize("cluster_mode", [True])
    async def test_sharded_pubsub_co_existence(self, request, cluster_mode: bool):
        """
        Test sharded PUBSUB with co-existence of multiple messages.

        This test verifies the behavior of sharded PUBSUB when multiple messages are published
        to the same sharded channel. It ensures that both async and sync methods of message retrieval
        function correctly in this scenario.

        It covers the scenario where messages are published to a sharded channel and received using
        both async and sync methods. This ensures that the asynchronous and synchronous message
        retrieval methods can coexist without interfering with each other and operate as expected.
        """
        listening_client, publishing_client = None, None
        try:
            channel = get_random_string(10)
            message = get_random_string(5)
            message2 = get_random_string(7)

            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {GlideClusterClientConfiguration.PubSubChannelModes.Sharded: {channel}},
                {},
            )

            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub
            )

            assert (
                await cast(GlideClusterClient, publishing_client).publish(
                    message, channel, sharded=True
                )
                == 1
            )
            assert (
                await cast(GlideClusterClient, publishing_client).publish(
                    message2, channel, sharded=True
                )
                == 1
            )

            # allow the messages to propagate
            await anyio.sleep(1)

            async_msg_res = await listening_client.get_pubsub_message()
            sync_msg_res = listening_client.try_get_pubsub_message()
            assert sync_msg_res
            async_msg = decode_pubsub_msg(async_msg_res)
            sync_msg = decode_pubsub_msg(sync_msg_res)

            assert async_msg.message in [message, message2]
            assert async_msg.channel == channel
            assert async_msg.pattern is None

            assert sync_msg.message in [message, message2]
            assert sync_msg.channel == channel
            assert sync_msg.pattern is None
            # we do not check the order of the messages, but we can check that we received both messages once
            assert not sync_msg.message == async_msg.message

            # assert there are no messages to read
            with pytest.raises(TimeoutError):
                with anyio.fail_after(3):
                    await listening_client.get_pubsub_message()

            assert listening_client.try_get_pubsub_message() is None
        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.skip_if_version_below("7.0.0")
    @pytest.mark.parametrize("cluster_mode", [True])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_sharded_pubsub_many_channels(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Test sharded PUBSUB with multiple channels and different message retrieval methods.

        This test verifies the behavior of sharded PUBSUB when multiple messages are published
        across multiple sharded channels. It covers three different message retrieval methods:
        Async, Sync, and Callback.
        """
        listening_client, publishing_client = None, None
        try:
            NUM_CHANNELS = 256
            shard_prefix = "{same-shard}"
            publish_response = 1

            # Create a map of channels to random messages with shard prefix
            channels_and_messages = {
                f"{shard_prefix}{get_random_string(10)}": get_random_string(5)
                for _ in range(NUM_CHANNELS)
            }

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Sharded: set(
                        channels_and_messages.keys()
                    )
                },
                {},
                callback=callback,
                context=context,
            )

            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub
            )

            # Publish messages to each channel
            for channel, message in channels_and_messages.items():
                assert (
                    await cast(GlideClusterClient, publishing_client).publish(
                        message, channel, sharded=True
                    )
                    == publish_response
                )

            # Allow the messages to propagate
            await anyio.sleep(1)

            # Check if all messages are received correctly
            for index in range(len(channels_and_messages)):
                pubsub_msg = await get_message_by_method(
                    method, listening_client, callback_messages, index
                )
                assert pubsub_msg.channel in channels_and_messages.keys()
                assert pubsub_msg.message == channels_and_messages[pubsub_msg.channel]
                assert pubsub_msg.pattern is None
                del channels_and_messages[pubsub_msg.channel]

            # check that we received all messages
            assert channels_and_messages == {}

            # Assert there are no more messages to read
            await check_no_messages_left(
                method, listening_client, callback_messages, NUM_CHANNELS
            )

        finally:
            if listening_client:
                await client_cleanup(listening_client)
            if publishing_client:
                await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_pubsub_pattern(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Test PUBSUB with pattern subscription using different message retrieval methods.

        This test verifies the behavior of PUBSUB when subscribing to a pattern and receiving
        messages using three different methods: Async, Sync, and Callback.
        """
        listening_client, publishing_client = None, None
        try:
            PATTERN = "{{{}}}:{}".format("channel", "*")
            channels = {
                "{{{}}}:{}".format("channel", get_random_string(5)): get_random_string(
                    5
                ),
                "{{{}}}:{}".format("channel", get_random_string(5)): get_random_string(
                    5
                ),
            }

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {GlideClusterClientConfiguration.PubSubChannelModes.Pattern: {PATTERN}},
                {GlideClientConfiguration.PubSubChannelModes.Pattern: {PATTERN}},
                callback=callback,
                context=context,
            )
            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub
            )

            for channel, message in channels.items():
                result = await publishing_client.publish(message, channel)
                if cluster_mode:
                    assert result == 1

            # allow the message to propagate
            await anyio.sleep(1)

            # Check if all messages are received correctly
            for index in range(len(channels)):
                pubsub_msg = await get_message_by_method(
                    method, listening_client, callback_messages, index
                )
                assert pubsub_msg.channel in channels.keys()
                assert pubsub_msg.message == channels[pubsub_msg.channel]
                assert pubsub_msg.pattern == PATTERN
                del channels[pubsub_msg.channel]

            # check that we received all messages
            assert channels == {}

            await check_no_messages_left(method, listening_client, callback_messages, 2)

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    async def test_pubsub_pattern_co_existence(self, request, cluster_mode: bool):
        """
        Tests the coexistence of async and sync message retrieval methods in pattern-based PUBSUB.

        This test covers the scenario where messages are published to a channel that match a specified pattern
        and received using both async and sync methods to ensure that both methods
        can coexist and function correctly.
        """
        listening_client, publishing_client = None, None
        try:
            PATTERN = "{{{}}}:{}".format("channel", "*")
            channels = {
                "{{{}}}:{}".format("channel", get_random_string(5)): get_random_string(
                    5
                ),
                "{{{}}}:{}".format("channel", get_random_string(5)): get_random_string(
                    5
                ),
            }

            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {GlideClusterClientConfiguration.PubSubChannelModes.Pattern: {PATTERN}},
                {GlideClientConfiguration.PubSubChannelModes.Pattern: {PATTERN}},
            )

            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub
            )

            for channel, message in channels.items():
                result = await publishing_client.publish(message, channel)
                if cluster_mode:
                    assert result == 1

            # allow the message to propagate
            await anyio.sleep(1)

            # Check if all messages are received correctly by each method
            for index in range(len(channels)):
                method = MethodTesting.Async if index % 2 else MethodTesting.Sync
                pubsub_msg = await get_message_by_method(method, listening_client)

                assert pubsub_msg.channel in channels.keys()
                assert pubsub_msg.message == channels[pubsub_msg.channel]
                assert pubsub_msg.pattern == PATTERN
                del channels[pubsub_msg.channel]

            # check that we received all messages
            assert channels == {}

            # assert there are no more messages to read
            with pytest.raises(TimeoutError):
                with anyio.fail_after(3):
                    await listening_client.get_pubsub_message()

            assert listening_client.try_get_pubsub_message() is None

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_pubsub_pattern_many_channels(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Tests publishing and receiving messages across many channels in pattern-based PUBSUB.

        This test covers the scenario where messages are published to multiple channels that match a specified pattern
        and received. It verifies that messages are correctly published and received
        using different retrieval methods: async, sync, and callback.
        """
        listening_client, publishing_client = None, None
        try:
            NUM_CHANNELS = 256
            PATTERN = "{{{}}}:{}".format("channel", "*")
            channels = {
                "{{{}}}:{}".format("channel", get_random_string(5)): get_random_string(
                    5
                )
                for _ in range(NUM_CHANNELS)
            }

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {GlideClusterClientConfiguration.PubSubChannelModes.Pattern: {PATTERN}},
                {GlideClientConfiguration.PubSubChannelModes.Pattern: {PATTERN}},
                callback=callback,
                context=context,
            )
            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub
            )

            for channel, message in channels.items():
                result = await publishing_client.publish(message, channel)
                if cluster_mode:
                    assert result == 1

            # allow the message to propagate
            await anyio.sleep(1)

            # Check if all messages are received correctly
            for index in range(len(channels)):
                pubsub_msg = await get_message_by_method(
                    method, listening_client, callback_messages, index
                )
                assert pubsub_msg.channel in channels.keys()
                assert pubsub_msg.message == channels[pubsub_msg.channel]
                assert pubsub_msg.pattern == PATTERN
                del channels[pubsub_msg.channel]

            # check that we received all messages
            assert channels == {}

            await check_no_messages_left(
                method, listening_client, callback_messages, NUM_CHANNELS
            )

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_pubsub_combined_exact_and_pattern_one_client(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Tests combined exact and pattern PUBSUB with one client.

        This test verifies that a single client can correctly handle both exact and pattern PUBSUB
        subscriptions. It covers the following scenarios:
        - Subscribing to multiple channels with exact names and verifying message reception.
        - Subscribing to channels using a pattern and verifying message reception.
        - Ensuring that messages are correctly published and received using different retrieval methods
        (async, sync, callback).
        """
        listening_client, publishing_client = None, None
        try:
            NUM_CHANNELS = 256
            PATTERN = "{{{}}}:{}".format("pattern", "*")

            # Create dictionaries of channels and their corresponding messages
            exact_channels_and_messages = {
                "{{{}}}:{}:{}".format(
                    "channel", get_random_string(5), i
                ): get_random_string(10)
                for i in range(NUM_CHANNELS)
            }
            pattern_channels_and_messages = {
                "{{{}}}:{}:{}".format(
                    "pattern", get_random_string(5), i
                ): get_random_string(5)
                for i in range(NUM_CHANNELS)
            }

            all_channels_and_messages = {
                **exact_channels_and_messages,
                **pattern_channels_and_messages,
            }

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []

            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            # Setup PUBSUB for exact channels
            pub_sub_exact = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Exact: set(
                        exact_channels_and_messages.keys()
                    ),
                    GlideClusterClientConfiguration.PubSubChannelModes.Pattern: {
                        PATTERN
                    },
                },
                {
                    GlideClientConfiguration.PubSubChannelModes.Exact: set(
                        exact_channels_and_messages.keys()
                    ),
                    GlideClientConfiguration.PubSubChannelModes.Pattern: {PATTERN},
                },
                callback=callback,
                context=context,
            )

            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request,
                cluster_mode,
                pub_sub_exact,
            )

            # Publish messages to all channels
            for channel, message in all_channels_and_messages.items():
                result = await publishing_client.publish(message, channel)
                if cluster_mode:
                    assert result == 1

            # allow the message to propagate
            await anyio.sleep(1)

            # Check if all messages are received correctly
            for index in range(len(all_channels_and_messages)):
                pubsub_msg = await get_message_by_method(
                    method, listening_client, callback_messages, index
                )
                pattern = (
                    PATTERN
                    if pubsub_msg.channel in pattern_channels_and_messages.keys()
                    else None
                )
                assert pubsub_msg.channel in all_channels_and_messages.keys()
                assert (
                    pubsub_msg.message == all_channels_and_messages[pubsub_msg.channel]
                )
                assert pubsub_msg.pattern == pattern
                del all_channels_and_messages[pubsub_msg.channel]

            # check that we received all messages
            assert all_channels_and_messages == {}

            await check_no_messages_left(
                method, listening_client, callback_messages, NUM_CHANNELS * 2
            )
        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_pubsub_combined_exact_and_pattern_multiple_clients(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Tests combined exact and pattern PUBSUB with multiple clients, one for each subscription.

        This test verifies that separate clients can correctly handle both exact and pattern PUBSUB
        subscriptions. It covers the following scenarios:
        - Subscribing to multiple channels with exact names and verifying message reception.
        - Subscribing to channels using a pattern and verifying message reception.
        - Ensuring that messages are correctly published and received using different retrieval methods
        (async, sync, callback).
        - Verifying that no messages are left unread.
        - Properly unsubscribing from all channels to avoid interference with other tests.
        """
        (
            listening_client_exact,
            publishing_client,
            listening_client_pattern,
            client_dont_care,
        ) = (None, None, None, None)
        try:
            NUM_CHANNELS = 256
            PATTERN = "{{{}}}:{}".format("pattern", "*")

            # Create dictionaries of channels and their corresponding messages
            exact_channels_and_messages = {
                "{{{}}}:{}:{}".format(
                    "channel", get_random_string(5), i
                ): get_random_string(10)
                for i in range(NUM_CHANNELS)
            }
            pattern_channels_and_messages = {
                "{{{}}}:{}:{}".format(
                    "pattern", get_random_string(5), i
                ): get_random_string(5)
                for i in range(NUM_CHANNELS)
            }

            all_channels_and_messages = {
                **exact_channels_and_messages,
                **pattern_channels_and_messages,
            }

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []

            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            # Setup PUBSUB for exact channels
            pub_sub_exact = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Exact: set(
                        exact_channels_and_messages.keys()
                    )
                },
                {
                    GlideClientConfiguration.PubSubChannelModes.Exact: set(
                        exact_channels_and_messages.keys()
                    )
                },
                callback=callback,
                context=context,
            )

            (
                listening_client_exact,
                publishing_client,
            ) = await create_two_clients_with_pubsub(
                request,
                cluster_mode,
                pub_sub_exact,
            )

            callback_messages_pattern: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages_pattern

            # Setup PUBSUB for pattern channels
            pub_sub_pattern = create_pubsub_subscription(
                cluster_mode,
                {GlideClusterClientConfiguration.PubSubChannelModes.Pattern: {PATTERN}},
                {GlideClientConfiguration.PubSubChannelModes.Pattern: {PATTERN}},
                callback=callback,
                context=context,
            )

            (
                listening_client_pattern,
                client_dont_care,
            ) = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub_pattern
            )

            # Publish messages to all channels
            for channel, message in all_channels_and_messages.items():
                result = await publishing_client.publish(message, channel)
                if cluster_mode:
                    assert result == 1

            # allow the messages to propagate
            await anyio.sleep(1)

            # Verify messages for exact PUBSUB
            for index in range(len(exact_channels_and_messages)):
                pubsub_msg = await get_message_by_method(
                    method, listening_client_exact, callback_messages, index
                )
                assert pubsub_msg.channel in exact_channels_and_messages.keys()
                assert (
                    pubsub_msg.message
                    == exact_channels_and_messages[pubsub_msg.channel]
                )
                assert pubsub_msg.pattern is None
                del exact_channels_and_messages[pubsub_msg.channel]

            # check that we received all messages
            assert exact_channels_and_messages == {}

            # Verify messages for pattern PUBSUB
            for index in range(len(pattern_channels_and_messages)):
                pubsub_msg = await get_message_by_method(
                    method, listening_client_pattern, callback_messages_pattern, index
                )
                assert pubsub_msg.channel in pattern_channels_and_messages.keys()
                assert (
                    pubsub_msg.message
                    == pattern_channels_and_messages[pubsub_msg.channel]
                )
                assert pubsub_msg.pattern == PATTERN
                del pattern_channels_and_messages[pubsub_msg.channel]

            # check that we received all messages
            assert pattern_channels_and_messages == {}

            await check_no_messages_left(
                method, listening_client_exact, callback_messages, NUM_CHANNELS
            )
            await check_no_messages_left(
                method,
                listening_client_pattern,
                callback_messages_pattern,
                NUM_CHANNELS,
            )

        finally:
            await client_cleanup(listening_client_exact)
            await client_cleanup(publishing_client)
            await client_cleanup(listening_client_pattern)
            await client_cleanup(client_dont_care)

    @pytest.mark.skip_if_version_below("7.0.0")
    @pytest.mark.parametrize("cluster_mode", [True])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_pubsub_combined_exact_pattern_and_sharded_one_client(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Tests combined exact, pattern and sharded PUBSUB with one client.

        This test verifies that a single client can correctly handle both exact, pattern and sharded PUBSUB
        subscriptions. It covers the following scenarios:
        - Subscribing to multiple channels with exact names and verifying message reception.
        - Subscribing to channels using a pattern and verifying message reception.
        - Subscribing to channels using a with sharded subscription and verifying message reception.
        - Ensuring that messages are correctly published and received using different retrieval methods
        (async, sync, callback).
        """
        listening_client, publishing_client = None, None
        try:
            NUM_CHANNELS = 256
            PATTERN = "{{{}}}:{}".format("pattern", "*")
            SHARD_PREFIX = "{same-shard}"

            # Create dictionaries of channels and their corresponding messages
            exact_channels_and_messages = {
                "{{{}}}:{}:{}".format(
                    "channel", get_random_string(5), i
                ): get_random_string(10)
                for i in range(NUM_CHANNELS)
            }
            pattern_channels_and_messages = {
                "{{{}}}:{}:{}".format(
                    "pattern", get_random_string(5), i
                ): get_random_string(5)
                for i in range(NUM_CHANNELS)
            }
            sharded_channels_and_messages = {
                f"{SHARD_PREFIX}:{i}:{get_random_string(10)}": get_random_string(7)
                for i in range(NUM_CHANNELS)
            }

            publish_response = 1

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []

            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            # Setup PUBSUB for exact channels
            pub_sub_exact = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Exact: set(
                        exact_channels_and_messages.keys()
                    ),
                    GlideClusterClientConfiguration.PubSubChannelModes.Pattern: {
                        PATTERN
                    },
                    GlideClusterClientConfiguration.PubSubChannelModes.Sharded: set(
                        sharded_channels_and_messages.keys()
                    ),
                },
                {},
                callback=callback,
                context=context,
            )

            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request,
                cluster_mode,
                pub_sub_exact,
            )

            # Publish messages to all channels
            for channel, message in {
                **exact_channels_and_messages,
                **pattern_channels_and_messages,
            }.items():
                assert (
                    await publishing_client.publish(message, channel)
                    == publish_response
                )

            # Publish sharded messages to all channels
            for channel, message in sharded_channels_and_messages.items():
                assert (
                    await cast(GlideClusterClient, publishing_client).publish(
                        message, channel, sharded=True
                    )
                    == publish_response
                )

            # allow the messages to propagate
            await anyio.sleep(1)

            all_channels_and_messages = {
                **exact_channels_and_messages,
                **pattern_channels_and_messages,
                **sharded_channels_and_messages,
            }
            # Check if all messages are received correctly
            for index in range(len(all_channels_and_messages)):
                pubsub_msg = await get_message_by_method(
                    method, listening_client, callback_messages, index
                )
                pattern = (
                    PATTERN
                    if pubsub_msg.channel in pattern_channels_and_messages.keys()
                    else None
                )
                assert pubsub_msg.channel in all_channels_and_messages.keys()
                assert (
                    pubsub_msg.message == all_channels_and_messages[pubsub_msg.channel]
                )
                assert pubsub_msg.pattern == pattern
                del all_channels_and_messages[pubsub_msg.channel]

            # check that we received all messages
            assert all_channels_and_messages == {}

            await check_no_messages_left(
                method, listening_client, callback_messages, NUM_CHANNELS * 3
            )

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.skip_if_version_below("7.0.0")
    @pytest.mark.parametrize("cluster_mode", [True])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_pubsub_combined_exact_pattern_and_sharded_multi_client(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Tests combined exact, pattern and sharded PUBSUB with multiple clients, one for each subscription.

        This test verifies that separate clients can correctly handle exact, pattern and sharded PUBSUB
        subscriptions. It covers the following scenarios:
        - Subscribing to multiple channels with exact names and verifying message reception.
        - Subscribing to channels using a pattern and verifying message reception.
        - Subscribing to channels using a sharded subscription and verifying message reception.
        - Ensuring that messages are correctly published and received using different retrieval methods
        (async, sync, callback).
        - Verifying that no messages are left unread.
        - Properly unsubscribing from all channels to avoid interference with other tests.
        """
        (
            listening_client_exact,
            publishing_client,
            listening_client_pattern,
            listening_client_sharded,
        ) = (None, None, None, None)

        (
            pub_sub_exact,
            pub_sub_sharded,
            pub_sub_pattern,
        ) = (None, None, None)

        try:
            NUM_CHANNELS = 256
            PATTERN = "{{{}}}:{}".format("pattern", "*")
            SHARD_PREFIX = "{same-shard}"

            # Create dictionaries of channels and their corresponding messages
            exact_channels_and_messages = {
                "{{{}}}:{}:{}".format(
                    "channel", get_random_string(5), i
                ): get_random_string(10)
                for i in range(NUM_CHANNELS)
            }
            pattern_channels_and_messages = {
                "{{{}}}:{}:{}".format(
                    "pattern", get_random_string(5), i
                ): get_random_string(5)
                for i in range(NUM_CHANNELS)
            }
            sharded_channels_and_messages = {
                f"{SHARD_PREFIX}:{i}:{get_random_string(10)}": get_random_string(7)
                for i in range(NUM_CHANNELS)
            }

            publish_response = 1

            callback, context = None, None
            callback_messages_exact: List[PubSubMsg] = []
            callback_messages_pattern: List[PubSubMsg] = []
            callback_messages_sharded: List[PubSubMsg] = []

            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages_exact

            # Setup PUBSUB for exact channels
            pub_sub_exact = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Exact: set(
                        exact_channels_and_messages.keys()
                    )
                },
                {
                    GlideClientConfiguration.PubSubChannelModes.Exact: set(
                        exact_channels_and_messages.keys()
                    )
                },
                callback=callback,
                context=context,
            )

            (
                listening_client_exact,
                publishing_client,
            ) = await create_two_clients_with_pubsub(
                request,
                cluster_mode,
                pub_sub_exact,
            )

            if method == MethodTesting.Callback:
                context = callback_messages_pattern

            # Setup PUBSUB for pattern channels
            pub_sub_pattern = create_pubsub_subscription(
                cluster_mode,
                {GlideClusterClientConfiguration.PubSubChannelModes.Pattern: {PATTERN}},
                {GlideClientConfiguration.PubSubChannelModes.Pattern: {PATTERN}},
                callback=callback,
                context=context,
            )

            if method == MethodTesting.Callback:
                context = callback_messages_sharded

            pub_sub_sharded = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Sharded: set(
                        sharded_channels_and_messages.keys()
                    )
                },
                {},
                callback=callback,
                context=context,
            )

            (
                listening_client_pattern,
                listening_client_sharded,
            ) = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub_pattern, pub_sub_sharded
            )

            # Publish messages to all channels
            for channel, message in {
                **exact_channels_and_messages,
                **pattern_channels_and_messages,
            }.items():
                assert (
                    await publishing_client.publish(message, channel)
                    == publish_response
                )

            # Publish sharded messages to all channels
            for channel, message in sharded_channels_and_messages.items():
                assert (
                    await cast(GlideClusterClient, publishing_client).publish(
                        message, channel, sharded=True
                    )
                    == publish_response
                )

            # allow the messages to propagate
            await anyio.sleep(1)

            # Verify messages for exact PUBSUB
            for index in range(len(exact_channels_and_messages)):
                pubsub_msg = await get_message_by_method(
                    method, listening_client_exact, callback_messages_exact, index
                )
                assert pubsub_msg.channel in exact_channels_and_messages.keys()
                assert (
                    pubsub_msg.message
                    == exact_channels_and_messages[pubsub_msg.channel]
                )
                assert pubsub_msg.pattern is None
                del exact_channels_and_messages[pubsub_msg.channel]

            # check that we received all messages
            assert exact_channels_and_messages == {}

            # Verify messages for pattern PUBSUB
            for index in range(len(pattern_channels_and_messages)):
                pubsub_msg = await get_message_by_method(
                    method, listening_client_pattern, callback_messages_pattern, index
                )
                assert pubsub_msg.channel in pattern_channels_and_messages.keys()
                assert (
                    pubsub_msg.message
                    == pattern_channels_and_messages[pubsub_msg.channel]
                )
                assert pubsub_msg.pattern == PATTERN
                del pattern_channels_and_messages[pubsub_msg.channel]

            # check that we received all messages
            assert pattern_channels_and_messages == {}

            # Verify messages for shaded PUBSUB
            for index in range(len(sharded_channels_and_messages)):
                pubsub_msg = await get_message_by_method(
                    method, listening_client_sharded, callback_messages_sharded, index
                )
                assert pubsub_msg.channel in sharded_channels_and_messages.keys()
                assert (
                    pubsub_msg.message
                    == sharded_channels_and_messages[pubsub_msg.channel]
                )
                assert pubsub_msg.pattern is None
                del sharded_channels_and_messages[pubsub_msg.channel]

            # check that we received all messages
            assert sharded_channels_and_messages == {}

            await check_no_messages_left(
                method, listening_client_exact, callback_messages_exact, NUM_CHANNELS
            )
            await check_no_messages_left(
                method,
                listening_client_pattern,
                callback_messages_pattern,
                NUM_CHANNELS,
            )
            await check_no_messages_left(
                method,
                listening_client_sharded,
                callback_messages_sharded,
                NUM_CHANNELS,
            )

        finally:
            await client_cleanup(listening_client_exact)
            await client_cleanup(publishing_client)
            await client_cleanup(listening_client_pattern)
            await client_cleanup(listening_client_sharded)

    @pytest.mark.skip_if_version_below("7.0.0")
    @pytest.mark.parametrize("cluster_mode", [True])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_pubsub_combined_different_channels_with_same_name(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Tests combined PUBSUB with different channel modes using the same channel name.
        One publishing clients, 3 listening clients, one for each mode.

        This test verifies that separate clients can correctly handle subscriptions for exact, pattern, and sharded channels
        with the same name.
        It covers the following scenarios:
        - Subscribing to an exact channel and verifying message reception.
        - Subscribing to a pattern channel and verifying message reception.
        - Subscribing to a sharded channel and verifying message reception.
        - Ensuring that messages are correctly published and received using different retrieval methods
        (async, sync, callback).
        - Verifying that no messages are left unread.
        - Properly unsubscribing from all channels to avoid interference with other tests.
        """
        (
            listening_client_exact,
            publishing_client,
            listening_client_pattern,
            listening_client_sharded,
        ) = (None, None, None, None)

        (
            pub_sub_exact,
            pub_sub_sharded,
            pub_sub_pattern,
        ) = (None, None, None)

        try:
            CHANNEL_NAME = "same-channel-name"
            MESSAGE_EXACT = get_random_string(10)
            MESSAGE_PATTERN = get_random_string(7)
            MESSAGE_SHARDED = get_random_string(5)

            callback, context = None, None
            callback_messages_exact: List[PubSubMsg] = []
            callback_messages_pattern: List[PubSubMsg] = []
            callback_messages_sharded: List[PubSubMsg] = []

            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages_exact

            # Setup PUBSUB for exact channel
            pub_sub_exact = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Exact: {
                        CHANNEL_NAME
                    }
                },
                {GlideClientConfiguration.PubSubChannelModes.Exact: {CHANNEL_NAME}},
                callback=callback,
                context=context,
            )

            (
                listening_client_exact,
                publishing_client,
            ) = await create_two_clients_with_pubsub(
                request,
                cluster_mode,
                pub_sub_exact,
            )

            # Setup PUBSUB for pattern channel
            if method == MethodTesting.Callback:
                context = callback_messages_pattern

            # Setup PUBSUB for pattern channels
            pub_sub_pattern = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Pattern: {
                        CHANNEL_NAME
                    }
                },
                {GlideClientConfiguration.PubSubChannelModes.Pattern: {CHANNEL_NAME}},
                callback=callback,
                context=context,
            )

            if method == MethodTesting.Callback:
                context = callback_messages_sharded

            pub_sub_sharded = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Sharded: {
                        CHANNEL_NAME
                    }
                },
                {},
                callback=callback,
                context=context,
            )

            (
                listening_client_pattern,
                listening_client_sharded,
            ) = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub_pattern, pub_sub_sharded
            )

            # Publish messages to each channel
            assert await publishing_client.publish(MESSAGE_EXACT, CHANNEL_NAME) == 2
            assert await publishing_client.publish(MESSAGE_PATTERN, CHANNEL_NAME) == 2
            assert (
                await cast(GlideClusterClient, publishing_client).publish(
                    MESSAGE_SHARDED, CHANNEL_NAME, sharded=True
                )
                == 1
            )

            # allow the message to propagate
            await anyio.sleep(1)

            # Verify message for exact and pattern PUBSUB
            for client, callback, pattern in [  # type: ignore
                (listening_client_exact, callback_messages_exact, None),
                (listening_client_pattern, callback_messages_pattern, CHANNEL_NAME),
            ]:
                pubsub_msg = await get_message_by_method(method, client, callback, 0)  # type: ignore

                pubsub_msg2 = await get_message_by_method(method, client, callback, 1)  # type: ignore
                assert not pubsub_msg.message == pubsub_msg2.message
                assert pubsub_msg2.message in [MESSAGE_PATTERN, MESSAGE_EXACT]
                assert pubsub_msg.message in [MESSAGE_PATTERN, MESSAGE_EXACT]
                assert pubsub_msg.channel == pubsub_msg2.channel == CHANNEL_NAME
                assert pubsub_msg.pattern == pubsub_msg2.pattern == pattern

            # Verify message for sharded PUBSUB
            pubsub_msg_sharded = await get_message_by_method(
                method, listening_client_sharded, callback_messages_sharded, 0
            )
            assert pubsub_msg_sharded.message == MESSAGE_SHARDED
            assert pubsub_msg_sharded.channel == CHANNEL_NAME
            assert pubsub_msg_sharded.pattern is None

            await check_no_messages_left(
                method, listening_client_exact, callback_messages_exact, 2
            )
            await check_no_messages_left(
                method, listening_client_pattern, callback_messages_pattern, 2
            )
            await check_no_messages_left(
                method, listening_client_sharded, callback_messages_sharded, 1
            )

        finally:
            await client_cleanup(listening_client_exact)
            await client_cleanup(publishing_client)
            await client_cleanup(listening_client_pattern)
            await client_cleanup(listening_client_sharded)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_pubsub_two_publishing_clients_same_name(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Tests PUBSUB with two publishing clients using the same channel name.
        One client uses pattern subscription, the other uses exact.
        The clients publishes messages to each other, and to thyself.

        This test verifies that two separate clients can correctly publish to and handle subscriptions
        for exact and pattern channels with the same name. It covers the following scenarios:
        - Subscribing to an exact channel and verifying message reception.
        - Subscribing to a pattern channel and verifying message reception.
        - Ensuring that messages are correctly published and received using different retrieval methods
        (async, sync, callback).
        - Verifying that no messages are left unread.
        - Properly unsubscribing from all channels to avoid interference with other tests.
        """
        client_exact, client_pattern = None, None
        try:
            CHANNEL_NAME = "channel-name"
            MESSAGE_EXACT = get_random_string(10)
            MESSAGE_PATTERN = get_random_string(7)
            callback, context_exact, context_pattern = None, None, None
            callback_messages_exact: List[PubSubMsg] = []
            callback_messages_pattern: List[PubSubMsg] = []

            if method == MethodTesting.Callback:
                callback = new_message
                context_exact = callback_messages_exact
                context_pattern = callback_messages_pattern

            # Setup PUBSUB for exact channel
            pub_sub_exact = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Exact: {
                        CHANNEL_NAME
                    }
                },
                {GlideClientConfiguration.PubSubChannelModes.Exact: {CHANNEL_NAME}},
                callback=callback,
                context=context_exact,
            )
            # Setup PUBSUB for pattern channels
            pub_sub_pattern = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Pattern: {
                        CHANNEL_NAME
                    }
                },
                {GlideClientConfiguration.PubSubChannelModes.Pattern: {CHANNEL_NAME}},
                callback=callback,
                context=context_pattern,
            )

            client_exact, client_pattern = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub_exact, pub_sub_pattern
            )

            # Publish messages to each channel - both clients publishing
            for msg in [MESSAGE_EXACT, MESSAGE_PATTERN]:
                result = await client_pattern.publish(msg, CHANNEL_NAME)
                if cluster_mode:
                    assert result == 2

            # allow the message to propagate
            await anyio.sleep(1)

            # Verify message for exact and pattern PUBSUB
            for client, callback, pattern in [  # type: ignore
                (client_exact, callback_messages_exact, None),
                (client_pattern, callback_messages_pattern, CHANNEL_NAME),
            ]:
                pubsub_msg = await get_message_by_method(method, client, callback, 0)  # type: ignore

                pubsub_msg2 = await get_message_by_method(method, client, callback, 1)  # type: ignore
                assert not pubsub_msg.message == pubsub_msg2.message
                assert pubsub_msg2.message in [MESSAGE_PATTERN, MESSAGE_EXACT]
                assert pubsub_msg.message in [MESSAGE_PATTERN, MESSAGE_EXACT]
                assert pubsub_msg.channel == pubsub_msg2.channel == CHANNEL_NAME
                assert pubsub_msg.pattern == pubsub_msg2.pattern == pattern

            await check_no_messages_left(
                method, client_pattern, callback_messages_pattern, 2
            )
            await check_no_messages_left(
                method, client_exact, callback_messages_exact, 2
            )

        finally:
            await client_cleanup(client_exact)
            await client_cleanup(client_pattern)

    @pytest.mark.skip_if_version_below("7.0.0")
    @pytest.mark.parametrize("cluster_mode", [True])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_pubsub_three_publishing_clients_same_name_with_sharded(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Tests PUBSUB with 3 publishing clients using the same channel name.
        One client uses pattern subscription, one uses exact, and one uses sharded.

        This test verifies that 3 separate clients can correctly publish to and handle subscriptions
        for exact, sharded and pattern channels with the same name. It covers the following scenarios:
        - Subscribing to an exact channel and verifying message reception.
        - Subscribing to a pattern channel and verifying message reception.
        - Subscribing to a sharded channel and verifying message reception.
        - Ensuring that messages are correctly published and received using different retrieval methods
        (async, sync, callback).
        - Verifying that no messages are left unread.
        - Properly unsubscribing from all channels to avoid interference with other tests.
        """
        client_exact, client_pattern, client_sharded, client_dont_care = (
            None,
            None,
            None,
            None,
        )
        try:
            CHANNEL_NAME = "same-channel-name"
            MESSAGE_EXACT = get_random_string(10)
            MESSAGE_PATTERN = get_random_string(7)
            MESSAGE_SHARDED = get_random_string(5)
            publish_response = 2 if cluster_mode else OK
            callback, context_exact, context_pattern, context_sharded = (
                None,
                None,
                None,
                None,
            )
            callback_messages_exact: List[PubSubMsg] = []
            callback_messages_pattern: List[PubSubMsg] = []
            callback_messages_sharded: List[PubSubMsg] = []

            if method == MethodTesting.Callback:
                callback = new_message
                context_exact = callback_messages_exact
                context_pattern = callback_messages_pattern
                context_sharded = callback_messages_sharded

            # Setup PUBSUB for exact channel
            pub_sub_exact = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Exact: {
                        CHANNEL_NAME
                    }
                },
                {GlideClientConfiguration.PubSubChannelModes.Exact: {CHANNEL_NAME}},
                callback=callback,
                context=context_exact,
            )
            # Setup PUBSUB for pattern channels
            pub_sub_pattern = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Pattern: {
                        CHANNEL_NAME
                    }
                },
                {GlideClientConfiguration.PubSubChannelModes.Pattern: {CHANNEL_NAME}},
                callback=callback,
                context=context_pattern,
            )
            # Setup PUBSUB for pattern channels
            pub_sub_sharded = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Sharded: {
                        CHANNEL_NAME
                    }
                },
                {},
                callback=callback,
                context=context_sharded,
            )

            client_exact, client_pattern = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub_exact, pub_sub_pattern
            )
            client_sharded, client_dont_care = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub_sharded
            )

            # Publish messages to each channel - both clients publishing
            assert (
                await client_pattern.publish(MESSAGE_EXACT, CHANNEL_NAME)
                == publish_response
            )
            assert (
                await client_sharded.publish(MESSAGE_PATTERN, CHANNEL_NAME)
                == publish_response
            )
            assert (
                await cast(GlideClusterClient, client_exact).publish(
                    MESSAGE_SHARDED, CHANNEL_NAME, sharded=True
                )
                == 1
            )

            # allow the message to propagate
            await anyio.sleep(1)

            # Verify message for exact and pattern PUBSUB
            for client, callback, pattern in [  # type: ignore
                (client_exact, callback_messages_exact, None),
                (client_pattern, callback_messages_pattern, CHANNEL_NAME),
            ]:
                pubsub_msg = await get_message_by_method(method, client, callback, 0)  # type: ignore

                pubsub_msg2 = await get_message_by_method(method, client, callback, 1)  # type: ignore
                assert not pubsub_msg.message == pubsub_msg2.message
                assert pubsub_msg2.message in [MESSAGE_PATTERN, MESSAGE_EXACT]
                assert pubsub_msg.message in [MESSAGE_PATTERN, MESSAGE_EXACT]
                assert pubsub_msg.channel == pubsub_msg2.channel == CHANNEL_NAME
                assert pubsub_msg.pattern == pubsub_msg2.pattern == pattern

            msg = await get_message_by_method(
                method, client_sharded, callback_messages_sharded, 0
            )
            assert msg.message == MESSAGE_SHARDED
            assert msg.channel == CHANNEL_NAME
            assert msg.pattern is None

            await check_no_messages_left(
                method, client_pattern, callback_messages_pattern, 2
            )
            await check_no_messages_left(
                method, client_exact, callback_messages_exact, 2
            )
            await check_no_messages_left(
                method, client_sharded, callback_messages_sharded, 1
            )

        finally:
            await client_cleanup(client_exact)
            await client_cleanup(client_pattern)
            await client_cleanup(client_sharded)
            await client_cleanup(client_dont_care)

    @pytest.mark.skip(
        reason="This test requires special configuration for client-output-buffer-limit for valkey-server and timeouts seems "
        + "to vary across platforms and server versions"
    )
    @pytest.mark.parametrize("cluster_mode", [True, False])
    async def test_pubsub_exact_max_size_message(self, request, cluster_mode: bool):
        """
        Tests publishing and receiving maximum size messages in PUBSUB.

        This test verifies that very large messages (512MB - BulkString max size) can be published and received
        correctly in both cluster and standalone modes. It ensures that the PUBSUB system
        can handle maximum size messages without errors and that async and sync message
        retrieval methods can coexist and function correctly.

        The test covers the following scenarios:
        - Setting up PUBSUB subscription for a specific channel.
        - Publishing two maximum size messages to the channel.
        - Verifying that the messages are received correctly using both async and sync methods.
        - Ensuring that no additional messages are left after the expected messages are received.
        """
        channel = get_random_string(10)
        message = "1" * 512 * 1024 * 1024
        message2 = "2" * 512 * 1024 * 1024

        pub_sub = create_pubsub_subscription(
            cluster_mode,
            {GlideClusterClientConfiguration.PubSubChannelModes.Exact: {channel}},
            {GlideClientConfiguration.PubSubChannelModes.Exact: {channel}},
        )

        listening_client, publishing_client = await create_two_clients_with_pubsub(
            request,
            cluster_mode,
            pub_sub,
            timeout=10000,
        )

        try:
            result = await publishing_client.publish(message, channel)
            if cluster_mode:
                assert result == 1

            result = await publishing_client.publish(message2, channel)
            if cluster_mode:
                assert result == 1
            # allow the message to propagate
            await anyio.sleep(15)

            async_msg = await listening_client.get_pubsub_message()
            assert async_msg.message == message.encode()
            assert async_msg.channel == channel.encode()
            assert async_msg.pattern is None

            sync_msg = listening_client.try_get_pubsub_message()
            assert sync_msg
            assert sync_msg.message == message2.encode()
            assert sync_msg.channel == channel.encode()
            assert sync_msg.pattern is None

            # assert there are no messages to read
            with pytest.raises(TimeoutError):
                with anyio.fail_after(3):
                    await listening_client.get_pubsub_message()

            assert listening_client.try_get_pubsub_message() is None

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.skip_if_version_below("7.0.0")
    @pytest.mark.skip(
        reason="This test requires special configuration for client-output-buffer-limit for valkey-server and timeouts seems "
        + "to vary across platforms and server versions"
    )
    @pytest.mark.parametrize("cluster_mode", [True])
    async def test_pubsub_sharded_max_size_message(self, request, cluster_mode: bool):
        """
        Tests publishing and receiving maximum size messages in sharded PUBSUB.

        This test verifies that very large messages (512MB - BulkString max size) can be published and received
        correctly. It ensures that the PUBSUB system
        can handle maximum size messages without errors and that async and sync message
        retrieval methods can coexist and function correctly.

        The test covers the following scenarios:
        - Setting up PUBSUB subscription for a specific sharded channel.
        - Publishing two maximum size messages to the channel.
        - Verifying that the messages are received correctly using both async and sync methods.
        - Ensuring that no additional messages are left after the expected messages are received.
        """
        publishing_client, listening_client = None, None
        try:
            channel = get_random_string(10)
            message = "1" * 512 * 1024 * 1024
            message2 = "2" * 512 * 1024 * 1024

            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {GlideClusterClientConfiguration.PubSubChannelModes.Sharded: {channel}},
                {},
            )

            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request,
                cluster_mode,
                pub_sub,
                timeout=10000,
            )

            assert (
                await cast(GlideClusterClient, publishing_client).publish(
                    message, channel, sharded=True
                )
                == 1
            )

            assert (
                await cast(GlideClusterClient, publishing_client).publish(
                    message2, channel, sharded=True
                )
                == 1
            )

            # allow the message to propagate
            await anyio.sleep(15)

            async_msg = await listening_client.get_pubsub_message()
            sync_msg = listening_client.try_get_pubsub_message()
            assert sync_msg

            assert async_msg.message == message.encode()
            assert async_msg.channel == channel.encode()
            assert async_msg.pattern is None

            assert sync_msg.message == message2.encode()
            assert sync_msg.channel == channel.encode()
            assert sync_msg.pattern is None

            # assert there are no messages to read
            with pytest.raises(TimeoutError):
                with anyio.fail_after(3):
                    await listening_client.get_pubsub_message()

            assert listening_client.try_get_pubsub_message() is None

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.skip(
        reason="This test requires special configuration for client-output-buffer-limit for valkey-server and timeouts seems "
        + "to vary across platforms and server versions"
    )
    @pytest.mark.parametrize("cluster_mode", [True, False])
    async def test_pubsub_exact_max_size_message_callback(
        self, request, cluster_mode: bool
    ):
        """
        Tests publishing and receiving maximum size messages in exact PUBSUB with callback method.

        This test verifies that very large messages (512MB - BulkString max size) can be published and received
        correctly in both cluster and standalone modes. It ensures that the PUBSUB system
        can handle maximum size messages without errors and that the callback message
        retrieval method works as expected.

        The test covers the following scenarios:
        - Setting up PUBSUB subscription for a specific channel with a callback.
        - Publishing a maximum size message to the channel.
        - Verifying that the message is received correctly using the callback method.
        """
        listening_client, publishing_client = None, None
        try:
            channel = get_random_string(10)
            message = "0" * 12 * 1024 * 1024

            callback_messages: List[PubSubMsg] = []
            callback, context = new_message, callback_messages

            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {GlideClusterClientConfiguration.PubSubChannelModes.Exact: {channel}},
                {GlideClientConfiguration.PubSubChannelModes.Exact: {channel}},
                callback=callback,
                context=context,
            )

            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub, timeout=10000
            )

            result = await publishing_client.publish(message, channel)
            if cluster_mode:
                assert result == 1
            # allow the message to propagate
            await anyio.sleep(15)

            assert len(callback_messages) == 1

            assert callback_messages[0].message == message.encode()
            assert callback_messages[0].channel == channel.encode()
            assert callback_messages[0].pattern is None

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.skip_if_version_below("7.0.0")
    @pytest.mark.skip(
        reason="This test requires special configuration for client-output-buffer-limit for valkey-server and timeouts seems "
        + "to vary across platforms and server versions"
    )
    @pytest.mark.parametrize("cluster_mode", [True])
    async def test_pubsub_sharded_max_size_message_callback(
        self, request, cluster_mode: bool
    ):
        """
        Tests publishing and receiving maximum size messages in sharded PUBSUB with callback method.

        This test verifies that very large messages (512MB - BulkString max size) can be published and received
        correctly. It ensures that the PUBSUB system
        can handle maximum size messages without errors and that the callback message
        retrieval method works as expected.

        The test covers the following scenarios:
        - Setting up PUBSUB subscription for a specific sharded channel with a callback.
        - Publishing a maximum size message to the channel.
        - Verifying that the message is received correctly using the callback method.
        """
        publishing_client, listening_client = None, None
        try:
            channel = get_random_string(10)
            message = "0" * 512 * 1024 * 1024

            callback_messages: List[PubSubMsg] = []
            callback, context = new_message, callback_messages

            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {GlideClusterClientConfiguration.PubSubChannelModes.Sharded: {channel}},
                {},
                callback=callback,
                context=context,
            )

            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub, timeout=10000
            )

            assert (
                await cast(GlideClusterClient, publishing_client).publish(
                    message, channel, sharded=True
                )
                == 1
            )

            # allow the message to propagate
            await anyio.sleep(15)

            assert len(callback_messages) == 1

            assert callback_messages[0].message == message.encode()
            assert callback_messages[0].channel == channel.encode()
            assert callback_messages[0].pattern is None

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    async def test_pubsub_resp2_raise_an_error(self, request, cluster_mode: bool):
        """Tests that when creating a resp2 client with PUBSUB - an error will be raised"""
        channel = get_random_string(5)

        pub_sub_exact = create_pubsub_subscription(
            cluster_mode,
            {GlideClusterClientConfiguration.PubSubChannelModes.Exact: {channel}},
            {GlideClientConfiguration.PubSubChannelModes.Exact: {channel}},
        )

        with pytest.raises(ConfigurationError):
            await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub_exact, protocol=ProtocolVersion.RESP2
            )

    @pytest.mark.parametrize("cluster_mode", [True, False])
    async def test_pubsub_context_with_no_callback_raise_error(
        self, request, cluster_mode: bool
    ):
        """Tests that when creating a PUBSUB client in callback method with context but no callback raises an error"""
        channel = get_random_string(5)
        context: List[PubSubMsg] = []
        pub_sub_exact = create_pubsub_subscription(
            cluster_mode,
            {GlideClusterClientConfiguration.PubSubChannelModes.Exact: {channel}},
            {GlideClientConfiguration.PubSubChannelModes.Exact: {channel}},
            context=context,
        )

        with pytest.raises(ConfigurationError):
            await create_two_clients_with_pubsub(request, cluster_mode, pub_sub_exact)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    async def test_pubsub_channels(self, request, cluster_mode: bool):
        """
        Tests the pubsub_channels command functionality.

        This test verifies that the pubsub_channels command correctly returns
        the active channels matching a specified pattern.
        """
        client1, client2, client = None, None, None
        try:
            channel1 = "test_channel1"
            channel2 = "test_channel2"
            channel3 = "some_channel3"
            pattern = "test_*"

            client = await create_client(request, cluster_mode)
            # Assert no channels exists yet
            assert await client.pubsub_channels() == []

            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Exact: {
                        channel1,
                        channel2,
                        channel3,
                    }
                },
                {
                    GlideClientConfiguration.PubSubChannelModes.Exact: {
                        channel1,
                        channel2,
                        channel3,
                    }
                },
            )

            channel1_bytes = channel1.encode()
            channel2_bytes = channel2.encode()
            channel3_bytes = channel3.encode()

            client1, client2 = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub
            )

            # Test pubsub_channels without pattern
            channels = await client2.pubsub_channels()
            assert set(channels) == {channel1_bytes, channel2_bytes, channel3_bytes}

            # Test pubsub_channels with pattern
            channels_with_pattern = await client2.pubsub_channels(pattern)
            assert set(channels_with_pattern) == {channel1_bytes, channel2_bytes}

            # Test with non-matching pattern
            non_matching_channels = await client2.pubsub_channels("non_matching_*")
            assert len(non_matching_channels) == 0

        finally:
            await client_cleanup(client1)
            await client_cleanup(client2)
            await client_cleanup(client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    async def test_pubsub_numpat(self, request, cluster_mode: bool):
        """
        Tests the pubsub_numpat command functionality.

        This test verifies that the pubsub_numpat command correctly returns
        the number of unique patterns that are subscribed to by clients.
        """
        client1, client2, client = None, None, None
        try:
            pattern1 = "test_*"
            pattern2 = "another_*"

            # Create a client and check initial number of patterns
            client = await create_client(request, cluster_mode)
            assert await client.pubsub_numpat() == 0

            # Set up subscriptions with patterns
            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Pattern: {
                        pattern1,
                        pattern2,
                    }
                },
                {
                    GlideClientConfiguration.PubSubChannelModes.Pattern: {
                        pattern1,
                        pattern2,
                    }
                },
            )

            client1, client2 = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub
            )

            # Test pubsub_numpat
            num_patterns = await client2.pubsub_numpat()
            assert num_patterns == 2

        finally:
            await client_cleanup(client1)
            await client_cleanup(client2)
            await client_cleanup(client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    async def test_pubsub_numsub(self, request, cluster_mode: bool):
        """
        Tests the pubsub_numsub command functionality.

        This test verifies that the pubsub_numsub command correctly returns
        the number of subscribers for specified channels.
        """
        client1, client2, client3, client4, client = None, None, None, None, None
        try:
            channel1 = "test_channel1"
            channel2 = "test_channel2"
            channel3 = "test_channel3"
            channel4 = "test_channel4"

            # Set up subscriptions
            pub_sub1 = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Exact: {
                        channel1,
                        channel2,
                        channel3,
                    }
                },
                {
                    GlideClientConfiguration.PubSubChannelModes.Exact: {
                        channel1,
                        channel2,
                        channel3,
                    }
                },
            )
            pub_sub2 = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Exact: {
                        channel2,
                        channel3,
                    }
                },
                {
                    GlideClientConfiguration.PubSubChannelModes.Exact: {
                        channel2,
                        channel3,
                    }
                },
            )
            pub_sub3 = create_pubsub_subscription(
                cluster_mode,
                {GlideClusterClientConfiguration.PubSubChannelModes.Exact: {channel3}},
                {GlideClientConfiguration.PubSubChannelModes.Exact: {channel3}},
            )

            channel1_bytes = channel1.encode()
            channel2_bytes = channel2.encode()
            channel3_bytes = channel3.encode()
            channel4_bytes = channel4.encode()

            # Create a client and check initial subscribers
            client = await create_client(request, cluster_mode)
            assert await client.pubsub_numsub([channel1, channel2, channel3]) == {
                channel1_bytes: 0,
                channel2_bytes: 0,
                channel3_bytes: 0,
            }

            client1, client2 = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub1, pub_sub2
            )
            client3, client4 = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub3
            )

            # Test pubsub_numsub
            subscribers = await client2.pubsub_numsub(
                [channel1_bytes, channel2_bytes, channel3_bytes, channel4_bytes]
            )
            assert subscribers == {
                channel1_bytes: 1,
                channel2_bytes: 2,
                channel3_bytes: 3,
                channel4_bytes: 0,
            }

            # Test pubsub_numsub with no channels
            empty_subscribers = await client2.pubsub_numsub()
            assert empty_subscribers == {}

        finally:
            await client_cleanup(client1)
            await client_cleanup(client2)
            await client_cleanup(client3)
            await client_cleanup(client4)
            await client_cleanup(client)

    @pytest.mark.skip_if_version_below("7.0.0")
    @pytest.mark.parametrize("cluster_mode", [True])
    async def test_pubsub_shardchannels(self, request, cluster_mode: bool):
        """
        Tests the pubsub_shardchannels command functionality.

        This test verifies that the pubsub_shardchannels command correctly returns
        the active sharded channels matching a specified pattern.
        """
        pub_sub, client1, client2, client = None, None, None, None
        try:
            channel1 = "test_shardchannel1"
            channel2 = "test_shardchannel2"
            channel3 = "some_shardchannel3"
            pattern = "test_*"

            client = await create_client(request, cluster_mode)
            assert isinstance(client, GlideClusterClient)
            # Assert no sharded channels exist yet
            assert await client.pubsub_shardchannels() == []

            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Sharded: {
                        channel1,
                        channel2,
                        channel3,
                    }
                },
                {},  # Empty dict for non-cluster mode as sharded channels are not supported
            )

            channel1_bytes = channel1.encode()
            channel2_bytes = channel2.encode()
            channel3_bytes = channel3.encode()

            client1, client2 = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub
            )

            assert isinstance(client2, GlideClusterClient)

            # Test pubsub_shardchannels without pattern
            channels = await client2.pubsub_shardchannels()
            assert set(channels) == {channel1_bytes, channel2_bytes, channel3_bytes}

            # Test pubsub_shardchannels with pattern
            channels_with_pattern = await client2.pubsub_shardchannels(pattern)
            assert set(channels_with_pattern) == {channel1_bytes, channel2_bytes}

            # Test with non-matching pattern
            assert await client2.pubsub_shardchannels("non_matching_*") == []

        finally:
            await client_cleanup(client1)
            await client_cleanup(client2)
            await client_cleanup(client)

    @pytest.mark.skip_if_version_below("7.0.0")
    @pytest.mark.parametrize("cluster_mode", [True])
    async def test_pubsub_shardnumsub(self, request, cluster_mode: bool):
        """
        Tests the pubsub_shardnumsub command functionality.

        This test verifies that the pubsub_shardnumsub command correctly returns
        the number of subscribers for specified sharded channels.
        """
        client1, client2, client3, client4, client = None, None, None, None, None
        try:
            channel1 = "test_shardchannel1"
            channel2 = "test_shardchannel2"
            channel3 = "test_shardchannel3"
            channel4 = "test_shardchannel4"

            # Set up subscriptions
            pub_sub1 = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Sharded: {
                        channel1,
                        channel2,
                        channel3,
                    }
                },
                {},
            )
            pub_sub2 = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Sharded: {
                        channel2,
                        channel3,
                    }
                },
                {},
            )
            pub_sub3 = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Sharded: {
                        channel3
                    }
                },
                {},
            )

            channel1_bytes = channel1.encode()
            channel2_bytes = channel2.encode()
            channel3_bytes = channel3.encode()
            channel4_bytes = channel4.encode()

            # Create a client and check initial subscribers
            client = await create_client(request, cluster_mode)

            assert isinstance(client, GlideClusterClient)
            assert await client.pubsub_shardnumsub([channel1, channel2, channel3]) == {
                channel1_bytes: 0,
                channel2_bytes: 0,
                channel3_bytes: 0,
            }

            client1, client2 = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub1, pub_sub2
            )

            client3, client4 = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub3
            )

            assert isinstance(client4, GlideClusterClient)

            # Test pubsub_shardnumsub
            subscribers = await client4.pubsub_shardnumsub(
                [channel1, channel2, channel3, channel4]
            )
            assert subscribers == {
                channel1_bytes: 1,
                channel2_bytes: 2,
                channel3_bytes: 3,
                channel4_bytes: 0,
            }

            # Test pubsub_shardnumsub with no channels
            empty_subscribers = await client4.pubsub_shardnumsub()
            assert empty_subscribers == {}

        finally:
            await client_cleanup(client1)
            await client_cleanup(client2)
            await client_cleanup(client3)
            await client_cleanup(client4)
            await client_cleanup(client)

    @pytest.mark.skip_if_version_below("7.0.0")
    @pytest.mark.parametrize("cluster_mode", [True])
    async def test_pubsub_channels_and_shardchannels_separation(
        self, request, cluster_mode: bool
    ):
        """
        Tests that pubsub_channels doesn't return sharded channels and pubsub_shardchannels
        doesn't return regular channels.
        """
        client1, client2 = None, None
        try:
            regular_channel = "regular_channel"
            shard_channel = "shard_channel"

            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Exact: {
                        regular_channel
                    },
                    GlideClusterClientConfiguration.PubSubChannelModes.Sharded: {
                        shard_channel
                    },
                },
                {GlideClientConfiguration.PubSubChannelModes.Exact: {regular_channel}},
            )

            regular_channel_bytes, shard_channel_bytes = (
                regular_channel.encode(),
                shard_channel.encode(),
            )

            client1, client2 = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub
            )

            assert isinstance(client2, GlideClusterClient)
            # Test pubsub_channels
            assert await client2.pubsub_channels() == [regular_channel_bytes]

            # Test pubsub_shardchannels
            assert await client2.pubsub_shardchannels() == [shard_channel_bytes]

        finally:
            await client_cleanup(client1)
            await client_cleanup(client2)

    @pytest.mark.skip_if_version_below("7.0.0")
    @pytest.mark.parametrize("cluster_mode", [True])
    async def test_pubsub_numsub_and_shardnumsub_separation(
        self, request, cluster_mode: bool
    ):
        """
        Tests that pubsub_numsub doesn't count sharded channel subscribers and pubsub_shardnumsub
        doesn't count regular channel subscribers.
        """
        client1, client2 = None, None
        try:
            regular_channel = "regular_channel"
            shard_channel = "shard_channel"

            pub_sub1 = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Exact: {
                        regular_channel
                    },
                    GlideClusterClientConfiguration.PubSubChannelModes.Sharded: {
                        shard_channel
                    },
                },
                {},
            )
            pub_sub2 = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Exact: {
                        regular_channel
                    },
                    GlideClusterClientConfiguration.PubSubChannelModes.Sharded: {
                        shard_channel
                    },
                },
                {},
            )

            regular_channel_bytes: bytes = regular_channel.encode()
            shard_channel_bytes: bytes = shard_channel.encode()

            client1, client2 = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub1, pub_sub2
            )

            assert isinstance(client2, GlideClusterClient)

            # Test pubsub_numsub
            regular_subscribers = await client2.pubsub_numsub(
                [regular_channel_bytes, shard_channel_bytes]
            )

            assert regular_subscribers == {
                regular_channel_bytes: 2,
                shard_channel_bytes: 0,
            }

            # Test pubsub_shardnumsub
            shard_subscribers = await client2.pubsub_shardnumsub(
                [regular_channel_bytes, shard_channel_bytes]
            )

            assert shard_subscribers == {
                regular_channel_bytes: 0,
                shard_channel_bytes: 2,
            }

        finally:
            await client_cleanup(client1)
            await client_cleanup(client2)


@pytest.mark.anyio
class TestDynamicPubSub:
    """Tests for dynamic PubSub subscription/unsubscription API"""

    @pytest.fixture(autouse=True)
    async def reset_broker(self):
        """Reset broker before each test in this class"""
        # TODO: remove when mock pubsub is removed
        yield
        MockPubSubBroker.reset()

    @pytest.mark.parametrize("cluster_mode", [True, False])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_subscribe_basic(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Test basic subscription using the subscribe() API.
        Client starts with no subscriptions, then subscribes.
        """
        listening_client, publishing_client = None, None
        try:
            channel = get_random_string(10)
            message = get_random_string(5)

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            listening_client = await create_client(
                request,
                cluster_mode,
                cluster_mode_pubsub=(
                    GlideClusterClientConfiguration.PubSubSubscriptions(
                        channels_and_patterns={},
                        callback=callback,
                        context=context,
                    )
                    if cluster_mode and callback
                    else None
                ),
                standalone_mode_pubsub=(
                    GlideClientConfiguration.PubSubSubscriptions(
                        channels_and_patterns={},
                        callback=callback,
                        context=context,
                    )
                    if not cluster_mode and callback
                    else None
                ),
            )
            publishing_client = await create_client(request, cluster_mode)

            await wait_for_subscription_state(
                listening_client,
                expected_channels=set(),
                expected_patterns=set(),
            )

            result = await listening_client.subscribe([channel])
            assert result == OK

            # Verify subscription is active
            await wait_for_subscription_state(
                listening_client, expected_channels={channel}
            )

            await publishing_client.publish(message, channel)
            await anyio.sleep(1)

            pubsub_msg = await get_message_by_method(
                method, listening_client, callback_messages, 0
            )
            assert pubsub_msg.message == message
            assert pubsub_msg.channel == channel
            assert pubsub_msg.pattern is None

            await check_no_messages_left(method, listening_client, callback_messages, 1)

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_unsubscribe_basic(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Test basic unsubscription using the unsubscribe() API.
        Client subscribes, then unsubscribes and verifies no messages received.
        """
        listening_client, publishing_client = None, None
        try:
            channel = get_random_string(10)
            message1 = get_random_string(5)
            message2 = get_random_string(5)

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {GlideClusterClientConfiguration.PubSubChannelModes.Exact: {channel}},
                {GlideClientConfiguration.PubSubChannelModes.Exact: {channel}},
                callback=callback,
                context=context,
            )
            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub
            )

            # Verify subscription is active
            await wait_for_subscription_state(
                listening_client, expected_channels={channel}
            )

            await publishing_client.publish(message1, channel)
            await anyio.sleep(1)
            pubsub_msg = await get_message_by_method(
                method, listening_client, callback_messages, 0
            )
            assert pubsub_msg.message == message1

            result = await listening_client.unsubscribe([channel])
            assert result == OK

            await wait_for_subscription_state(listening_client, expected_channels=set())

            # Publish second message - should not be received
            await publishing_client.publish(message2, channel)
            await anyio.sleep(1)

            await check_no_messages_left(method, listening_client, callback_messages, 1)

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_psubscribe_basic(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Test basic pattern subscription using the psubscribe() API.
        """
        listening_client, publishing_client = None, None
        try:
            pattern = "news.*"
            channel1 = "news.sports"
            message = get_random_string(5)

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            # Create clients without initial subscriptions
            listening_client = await create_client(
                request,
                cluster_mode,
                cluster_mode_pubsub=(
                    GlideClusterClientConfiguration.PubSubSubscriptions(
                        channels_and_patterns={},
                        callback=callback,
                        context=context,
                    )
                    if cluster_mode and callback
                    else None
                ),
                standalone_mode_pubsub=(
                    GlideClientConfiguration.PubSubSubscriptions(
                        channels_and_patterns={},
                        callback=callback,
                        context=context,
                    )
                    if not cluster_mode and callback
                    else None
                ),
            )
            publishing_client = await create_client(request, cluster_mode)

            # Verify no subscriptions initially
            await wait_for_subscription_state(
                listening_client,
                expected_channels=set(),
                expected_patterns=set(),
            )

            result = await listening_client.psubscribe([pattern])
            assert result == OK

            # Verify pattern subscription is active
            await wait_for_subscription_state(
                listening_client, expected_patterns={pattern}
            )

            await publishing_client.publish(message, channel1)
            await anyio.sleep(1)

            pubsub_msg = await get_message_by_method(
                method, listening_client, callback_messages, 0
            )
            assert pubsub_msg.message == message
            assert pubsub_msg.channel == channel1
            assert pubsub_msg.pattern == pattern

            await check_no_messages_left(method, listening_client, callback_messages, 1)

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_punsubscribe_basic(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Test basic pattern unsubscription using the punsubscribe() API.
        """
        listening_client, publishing_client = None, None
        try:
            pattern = "news.*"
            channel = "news.sports"
            message1 = get_random_string(5)
            message2 = get_random_string(5)

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {GlideClusterClientConfiguration.PubSubChannelModes.Pattern: {pattern}},
                {GlideClientConfiguration.PubSubChannelModes.Pattern: {pattern}},
                callback=callback,
                context=context,
            )
            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub
            )

            # Verify pattern subscription is active
            await wait_for_subscription_state(
                listening_client, expected_patterns={pattern}
            )

            await publishing_client.publish(message1, channel)
            await anyio.sleep(1)
            pubsub_msg = await get_message_by_method(
                method, listening_client, callback_messages, 0
            )
            assert pubsub_msg.message == message1

            result = await listening_client.punsubscribe([pattern])
            assert result == OK

            await wait_for_subscription_state(listening_client, expected_patterns=set())

            # Publish second message - should not be received
            await publishing_client.publish(message2, channel)
            await anyio.sleep(1)

            await check_no_messages_left(method, listening_client, callback_messages, 1)

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.skip_if_version_below("7.0.0")
    @pytest.mark.parametrize("cluster_mode", [True])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_ssubscribe_basic(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Test basic sharded subscription using the ssubscribe() API.
        """
        listening_client, publishing_client = None, None
        try:
            channel = get_random_string(10)
            message = get_random_string(5)

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            listening_client = await create_client(
                request,
                cluster_mode,
                cluster_mode_pubsub=(
                    GlideClusterClientConfiguration.PubSubSubscriptions(
                        channels_and_patterns={},
                        callback=callback,
                        context=context,
                    )
                    if callback
                    else None
                ),
            )
            publishing_client = await create_client(request, cluster_mode)

            await wait_for_subscription_state(
                listening_client,
                expected_channels=set(),
                expected_sharded=set(),
            )

            result = await cast(GlideClusterClient, listening_client).ssubscribe(
                [channel]
            )
            assert result == OK

            # Verify sharded subscription is active
            await wait_for_subscription_state(
                listening_client, expected_sharded={channel}
            )

            await cast(GlideClusterClient, publishing_client).publish(
                message, channel, sharded=True
            )
            await anyio.sleep(1)

            pubsub_msg = await get_message_by_method(
                method, listening_client, callback_messages, 0
            )
            assert pubsub_msg.message == message
            assert pubsub_msg.channel == channel
            assert pubsub_msg.pattern is None

            await check_no_messages_left(method, listening_client, callback_messages, 1)

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.skip_if_version_below("7.0.0")
    @pytest.mark.parametrize("cluster_mode", [True])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_sunsubscribe_basic(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Test basic sharded unsubscription using the sunsubscribe() API.
        """
        listening_client, publishing_client = None, None
        try:
            channel = get_random_string(10)
            message1 = get_random_string(5)
            message2 = get_random_string(5)

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {GlideClusterClientConfiguration.PubSubChannelModes.Sharded: {channel}},
                {},
                callback=callback,
                context=context,
            )
            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub
            )

            await wait_for_subscription_state(
                listening_client, expected_sharded={channel}
            )

            await cast(GlideClusterClient, publishing_client).publish(
                message1, channel, sharded=True
            )
            await anyio.sleep(1)
            pubsub_msg = await get_message_by_method(
                method, listening_client, callback_messages, 0
            )
            assert pubsub_msg.message == message1

            result = await cast(GlideClusterClient, listening_client).sunsubscribe(
                [channel]
            )
            assert result == OK

            await wait_for_subscription_state(listening_client, expected_sharded=set())

            # Publish second message - should not be received
            await cast(GlideClusterClient, publishing_client).publish(
                message2, channel, sharded=True
            )
            await anyio.sleep(1)

            await check_no_messages_left(method, listening_client, callback_messages, 1)

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    async def test_subscribe_coexistence_async_sync(self, request, cluster_mode: bool):
        """
        Test that async and sync message retrieval can coexist for dynamically subscribed channels.
        """
        listening_client, publishing_client = None, None
        try:
            channel = get_random_string(10)
            message1 = get_random_string(5)
            message2 = get_random_string(5)

            listening_client = await create_client(request, cluster_mode)
            publishing_client = await create_client(request, cluster_mode)

            # Subscribe dynamically
            await listening_client.subscribe([channel])
            await wait_for_subscription_state(
                listening_client, expected_channels={channel}
            )

            # Publish two messages
            await publishing_client.publish(message1, channel)
            await publishing_client.publish(message2, channel)
            await anyio.sleep(1)

            # Retrieve using both async and sync methods
            async_msg = decode_pubsub_msg(await listening_client.get_pubsub_message())
            sync_msg = decode_pubsub_msg(listening_client.try_get_pubsub_message())

            assert async_msg.message in [message1, message2]
            assert sync_msg.message in [message1, message2]
            assert (
                async_msg.message != sync_msg.message
            )  # Both messages received, one by each method

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_subscribe_multiple_channels(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Test subscribing to multiple channels in a single subscribe() call.
        """
        listening_client, publishing_client = None, None
        try:
            channels = [get_random_string(10) for _ in range(3)]
            messages = {ch: get_random_string(5) for ch in channels}

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            # Create client with no initial subscriptions
            listening_client = await create_client(
                request,
                cluster_mode,
                cluster_mode_pubsub=(
                    GlideClusterClientConfiguration.PubSubSubscriptions(
                        channels_and_patterns={},
                        callback=callback,
                        context=context,
                    )
                    if cluster_mode and callback
                    else None
                ),
                standalone_mode_pubsub=(
                    GlideClientConfiguration.PubSubSubscriptions(
                        channels_and_patterns={},
                        callback=callback,
                        context=context,
                    )
                    if not cluster_mode and callback
                    else None
                ),
            )
            publishing_client = await create_client(request, cluster_mode)

            result = await listening_client.subscribe(channels)
            assert result == OK

            await wait_for_subscription_state(
                listening_client, expected_channels=set(channels)
            )

            for channel, message in messages.items():
                await publishing_client.publish(message, channel)

            await anyio.sleep(1)

            received_messages = {}
            for index in range(len(channels)):
                pubsub_msg = await get_message_by_method(
                    method, listening_client, callback_messages, index
                )
                received_messages[pubsub_msg.channel] = pubsub_msg.message

            assert received_messages == messages

            await check_no_messages_left(
                method, listening_client, callback_messages, len(channels)
            )

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_psubscribe_multiple_patterns(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Test subscribing to multiple patterns in a single psubscribe() call.
        """
        listening_client, publishing_client = None, None
        try:
            patterns = ["news.*", "updates.*", "alerts.*"]
            channels = {
                "news.sports": get_random_string(5),
                "updates.weather": get_random_string(5),
                "alerts.security": get_random_string(5),
            }

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            # Create clients without initial subscriptions
            listening_client = await create_client(
                request,
                cluster_mode,
                cluster_mode_pubsub=(
                    GlideClusterClientConfiguration.PubSubSubscriptions(
                        channels_and_patterns={},
                        callback=callback,
                        context=context,
                    )
                    if cluster_mode and callback
                    else None
                ),
                standalone_mode_pubsub=(
                    GlideClientConfiguration.PubSubSubscriptions(
                        channels_and_patterns={},
                        callback=callback,
                        context=context,
                    )
                    if not cluster_mode and callback
                    else None
                ),
            )
            publishing_client = await create_client(request, cluster_mode)

            result = await listening_client.psubscribe(cast(list[str | bytes], patterns))
            assert result == OK

            await wait_for_subscription_state(
                listening_client, expected_patterns=set(patterns)
            )

            for channel, message in channels.items():
                await publishing_client.publish(message, channel)

            await anyio.sleep(1)

            received = {}
            for index in range(len(channels)):
                pubsub_msg = await get_message_by_method(
                    method, listening_client, callback_messages, index
                )
                received[pubsub_msg.channel] = pubsub_msg.message
                assert pubsub_msg.pattern in patterns

            # Verify all channels received messages
            for channel, message in channels.items():
                assert channel in received
                assert received[channel] == message

            await check_no_messages_left(
                method, listening_client, callback_messages, len(channels)
            )

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.skip_if_version_below("7.0.0")
    @pytest.mark.parametrize("cluster_mode", [True])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_ssubscribe_multiple_channels(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Test subscribing to multiple sharded channels in a single ssubscribe() call.
        """
        listening_client, publishing_client = None, None
        try:
            channels = [get_random_string(10) for _ in range(3)]
            messages = {ch: get_random_string(5) for ch in channels}

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            # Create clients without initial subscriptions
            listening_client = await create_client(
                request,
                cluster_mode,
                cluster_mode_pubsub=(
                    GlideClusterClientConfiguration.PubSubSubscriptions(
                        channels_and_patterns={},
                        callback=callback,
                        context=context,
                    )
                    if callback
                    else None
                ),
            )
            publishing_client = await create_client(request, cluster_mode)

            result = await cast(GlideClusterClient, listening_client).ssubscribe(
                channels
            )
            assert result == OK

            await wait_for_subscription_state(
                listening_client, expected_sharded=set(channels)
            )

            for channel, message in messages.items():
                await cast(GlideClusterClient, publishing_client).publish(
                    message, channel, sharded=True
                )

            await anyio.sleep(1)

            received_messages = {}
            for index in range(len(channels)):
                pubsub_msg = await get_message_by_method(
                    method, listening_client, callback_messages, index
                )
                received_messages[pubsub_msg.channel] = pubsub_msg.message

            assert received_messages == messages

            await check_no_messages_left(
                method, listening_client, callback_messages, len(channels)
            )

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_unsubscribe_multiple_channels(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Test unsubscribing from multiple specific channels (not all).
        """
        listening_client, publishing_client = None, None
        try:
            channels = [get_random_string(10) for _ in range(5)]
            channels_to_unsub = channels[:3]  # Unsubscribe from first 3
            channels_remaining = channels[3:]  # Keep last 2

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            # Create client with initial subscriptions to all channels
            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Exact: set(
                        channels
                    )
                },
                {GlideClientConfiguration.PubSubChannelModes.Exact: set(channels)},
                callback=callback,
                context=context,
            )
            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub
            )

            await wait_for_subscription_state(
                listening_client, expected_channels=set(channels)
            )

            result = await listening_client.unsubscribe(channels_to_unsub)
            assert result == OK

            await wait_for_subscription_state(
                listening_client, expected_channels=set(channels_remaining)
            )

            # Publish to all original channels
            messages = {ch: get_random_string(5) for ch in channels}
            for channel, message in messages.items():
                await publishing_client.publish(message, channel)

            await anyio.sleep(1)

            # Should only receive messages from remaining channels
            received_channels = set()
            for index in range(len(channels_remaining)):
                pubsub_msg = await get_message_by_method(
                    method, listening_client, callback_messages, index
                )
                received_channels.add(pubsub_msg.channel)

            assert received_channels == set(channels_remaining)

            # Verify unsubscribed channels did NOT receive messages
            for ch in channels_to_unsub:
                assert ch not in received_channels

            await check_no_messages_left(
                method, listening_client, callback_messages, len(channels_remaining)
            )

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_punsubscribe_multiple_patterns(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Test unsubscribing from multiple specific patterns (not all).
        """
        listening_client, publishing_client = None, None
        try:
            patterns = ["news.*", "updates.*", "alerts.*", "info.*"]
            patterns_to_unsub = patterns[:2]  # Unsubscribe from first 2
            patterns_remaining = patterns[2:]  # Keep last 2

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            # Create client with initial pattern subscriptions
            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Pattern: set(
                        patterns
                    )
                },
                {GlideClientConfiguration.PubSubChannelModes.Pattern: set(patterns)},
                callback=callback,
                context=context,
            )
            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub
            )

            # Verify all pattern subscriptions are active
            await wait_for_subscription_state(
                listening_client, expected_patterns=set(patterns)
            )

            result = await listening_client.punsubscribe(cast(list[str | bytes], patterns_to_unsub))
            assert result == OK

            await wait_for_subscription_state(
                listening_client, expected_patterns=set(patterns_remaining)
            )

            # Publish to channels matching all original patterns
            channels = {
                "news.sports": get_random_string(5),
                "updates.weather": get_random_string(5),
                "alerts.security": get_random_string(5),
                "info.general": get_random_string(5),
            }
            for channel, message in channels.items():
                await publishing_client.publish(message, channel)

            await anyio.sleep(1)

            # Should only receive messages from remaining patterns
            received_count = 0
            for index in range(len(patterns_remaining)):
                pubsub_msg = await get_message_by_method(
                    method, listening_client, callback_messages, index
                )
                assert pubsub_msg.pattern in patterns_remaining
                received_count += 1

            assert received_count == len(patterns_remaining)

            await check_no_messages_left(
                method, listening_client, callback_messages, len(patterns_remaining)
            )

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.skip_if_version_below("7.0.0")
    @pytest.mark.parametrize("cluster_mode", [True])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_sunsubscribe_multiple_channels(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Test unsubscribing from multiple specific sharded channels (not all).
        """
        listening_client, publishing_client = None, None
        try:
            channels = [get_random_string(10) for _ in range(5)]
            channels_to_unsub = channels[:3]  # Unsubscribe from first 3
            channels_remaining = channels[3:]  # Keep last 2

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            # Create client with initial sharded subscriptions
            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Sharded: set(
                        channels
                    )
                },
                {},
                callback=callback,
                context=context,
            )
            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub
            )

            await wait_for_subscription_state(
                listening_client, expected_sharded=set(channels)
            )

            # Unsubscribe from multiple specific sharded channels
            result = await cast(GlideClusterClient, listening_client).sunsubscribe(
                channels_to_unsub
            )
            assert result == OK

            await wait_for_subscription_state(
                listening_client, expected_sharded=set(channels_remaining)
            )

            # Publish to all original channels
            messages = {ch: get_random_string(5) for ch in channels}
            for channel, message in messages.items():
                await cast(GlideClusterClient, publishing_client).publish(
                    message, channel, sharded=True
                )

            await anyio.sleep(1)

            # Should only receive messages from remaining channels
            received_channels = set()
            for index in range(len(channels_remaining)):
                pubsub_msg = await get_message_by_method(
                    method, listening_client, callback_messages, index
                )
                received_channels.add(pubsub_msg.channel)

            assert received_channels == set(channels_remaining)

            # Verify unsubscribed channels did NOT receive messages
            for ch in channels_to_unsub:
                assert ch not in received_channels

            await check_no_messages_left(
                method, listening_client, callback_messages, len(channels_remaining)
            )

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_unsubscribe_all_channels(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Test unsubscribing from all channels using unsubscribe() with no arguments.
        """
        listening_client, publishing_client = None, None
        try:
            channels = [get_random_string(10) for _ in range(3)]
            message = get_random_string(5)

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            # Create client with initial subscriptions
            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Exact: set(
                        channels
                    )
                },
                {GlideClientConfiguration.PubSubChannelModes.Exact: set(channels)},
                callback=callback,
                context=context,
            )
            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub
            )

            # Verify all subscriptions are active
            await wait_for_subscription_state(
                listening_client, expected_channels=set(channels)
            )

            # Unsubscribe from all channels
            result = await listening_client.unsubscribe()
            assert result == OK

            await wait_for_subscription_state(listening_client, expected_channels=set())

            # Publish to any channel - should not be received
            await publishing_client.publish(message, channels[0])
            await anyio.sleep(1)

            await check_no_messages_left(method, listening_client, callback_messages, 0)

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_punsubscribe_all_patterns(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Test unsubscribing from all patterns using punsubscribe() with no arguments.
        """
        listening_client, publishing_client = None, None
        try:
            patterns = ["news.*", "updates.*"]
            channel = "news.sports"
            message = get_random_string(5)

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            # Create client with initial pattern subscriptions
            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Pattern: set(
                        patterns
                    )
                },
                {GlideClientConfiguration.PubSubChannelModes.Pattern: set(patterns)},
                callback=callback,
                context=context,
            )
            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub
            )

            # Verify all pattern subscriptions are active
            await wait_for_subscription_state(
                listening_client, expected_patterns=set(patterns)
            )

            result = await listening_client.punsubscribe()
            assert result == OK

            await wait_for_subscription_state(listening_client, expected_patterns=set())

            # Publish to matching channel - should not be received
            await publishing_client.publish(message, channel)
            await anyio.sleep(1)

            await check_no_messages_left(method, listening_client, callback_messages, 0)

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.skip_if_version_below("7.0.0")
    @pytest.mark.parametrize("cluster_mode", [True])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_sunsubscribe_all_sharded(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Test unsubscribing from all sharded channels using sunsubscribe() with no arguments.
        """
        listening_client, publishing_client = None, None
        try:
            channels = [get_random_string(10) for _ in range(3)]
            message = get_random_string(5)

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            # Create client with initial sharded subscriptions
            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Sharded: set(
                        channels
                    )
                },
                {},
                callback=callback,
                context=context,
            )
            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub
            )

            # Verify all sharded subscriptions are active
            await wait_for_subscription_state(
                listening_client, expected_sharded=set(channels)
            )

            result = await cast(GlideClusterClient, listening_client).sunsubscribe()
            assert result == OK

            await wait_for_subscription_state(listening_client, expected_sharded=set())

            # Publish to any channel - should not be received
            await cast(GlideClusterClient, publishing_client).publish(
                message, channels[0], sharded=True
            )
            await anyio.sleep(1)

            await check_no_messages_left(method, listening_client, callback_messages, 0)

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_subscribe_many_channels(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Test subscribing to many channels (256) using dynamic API.
        Verifies the system can handle a large number of subscriptions.
        """
        listening_client, publishing_client = None, None
        try:
            NUM_CHANNELS = 256
            shard_prefix = "{same-shard}"

            # Create a map of channels to random messages with shard prefix
            channels_and_messages = {
                f"{shard_prefix}{get_random_string(10)}": get_random_string(5)
                for _ in range(NUM_CHANNELS)
            }

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            listening_client = await create_client(
                request,
                cluster_mode,
                cluster_mode_pubsub=(
                    GlideClusterClientConfiguration.PubSubSubscriptions(
                        channels_and_patterns={},
                        callback=callback,
                        context=context,
                    )
                    if cluster_mode and callback
                    else None
                ),
                standalone_mode_pubsub=(
                    GlideClientConfiguration.PubSubSubscriptions(
                        channels_and_patterns={},
                        callback=callback,
                        context=context,
                    )
                    if not cluster_mode and callback
                    else None
                ),
            )
            publishing_client = await create_client(request, cluster_mode)

            result = await listening_client.subscribe(
                list(channels_and_messages.keys())
            )
            assert result == OK

            # Verify all subscriptions are active
            await wait_for_subscription_state(
                listening_client, expected_channels=set(channels_and_messages.keys())
            )

            for channel, message in channels_and_messages.items():
                result = await publishing_client.publish(message, channel)
                if cluster_mode:
                    assert result == 1

            # Allow the messages to propagate
            await anyio.sleep(1)

            for index in range(len(channels_and_messages)):
                pubsub_msg = await get_message_by_method(
                    method, listening_client, callback_messages, index
                )
                assert pubsub_msg.channel in channels_and_messages.keys()
                assert pubsub_msg.message == channels_and_messages[pubsub_msg.channel]
                assert pubsub_msg.pattern is None
                del channels_and_messages[pubsub_msg.channel]

            # check that we received all messages
            assert channels_and_messages == {}
            # check no messages left
            await check_no_messages_left(
                method, listening_client, callback_messages, NUM_CHANNELS
            )

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_mixed_config_and_api_subscriptions(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Test mixing config-based subscriptions with API subscriptions.
        Verifies both types work together correctly.
        """
        listening_client, publishing_client = None, None
        try:
            config_channel = get_random_string(10)
            api_channel = get_random_string(10)
            message1 = get_random_string(5)
            message2 = get_random_string(5)

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            # Create client with config-based subscription
            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {
                    GlideClusterClientConfiguration.PubSubChannelModes.Exact: {
                        config_channel
                    }
                },
                {GlideClientConfiguration.PubSubChannelModes.Exact: {config_channel}},
                callback=callback,
                context=context,
            )
            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub
            )

            # Verify config subscription is active
            await wait_for_subscription_state(
                listening_client, expected_channels={config_channel}
            )

            result = await listening_client.subscribe([api_channel])
            assert result == OK

            # Verify both subscriptions are active
            await wait_for_subscription_state(
                listening_client, expected_channels={config_channel, api_channel}
            )

            await publishing_client.publish(message1, config_channel)
            await publishing_client.publish(message2, api_channel)
            await anyio.sleep(1)

            received = {}
            for index in range(2):
                pubsub_msg = await get_message_by_method(
                    method, listening_client, callback_messages, index
                )
                received[pubsub_msg.channel] = pubsub_msg.message

            assert config_channel in received
            assert api_channel in received
            assert received[config_channel] == message1
            assert received[api_channel] == message2

            await check_no_messages_left(method, listening_client, callback_messages, 2)

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_subscribe_then_unsubscribe_same_channel(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Test subscribing and then unsubscribing from the same channel.
        """
        listening_client, publishing_client = None, None
        try:
            channel = get_random_string(10)
            message1 = get_random_string(5)
            message2 = get_random_string(5)

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            # Create clients without initial subscriptions
            listening_client = await create_client(
                request,
                cluster_mode,
                cluster_mode_pubsub=(
                    GlideClusterClientConfiguration.PubSubSubscriptions(
                        channels_and_patterns={},
                        callback=callback,
                        context=context,
                    )
                    if cluster_mode and callback
                    else None
                ),
                standalone_mode_pubsub=(
                    GlideClientConfiguration.PubSubSubscriptions(
                        channels_and_patterns={},
                        callback=callback,
                        context=context,
                    )
                    if not cluster_mode and callback
                    else None
                ),
            )
            publishing_client = await create_client(request, cluster_mode)

            result = await listening_client.subscribe([channel])
            assert result == OK

            await wait_for_subscription_state(
                listening_client, expected_channels={channel}
            )

            await publishing_client.publish(message1, channel)
            await anyio.sleep(1)
            pubsub_msg = await get_message_by_method(
                method, listening_client, callback_messages, 0
            )
            assert pubsub_msg.message == message1

            result = await listening_client.unsubscribe([channel])
            assert result == OK

            await wait_for_subscription_state(listening_client, expected_channels=set())

            # Publish again and verify no message received
            await publishing_client.publish(message2, channel)
            await anyio.sleep(1)
            await check_no_messages_left(method, listening_client, callback_messages, 1)

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_subscribe_to_already_subscribed_channel(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Test subscribing to a channel that's already subscribed (idempotent).
        """
        listening_client, publishing_client = None, None
        try:
            channel = get_random_string(10)
            message = get_random_string(5)

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            # Create client with initial subscription
            pub_sub = create_pubsub_subscription(
                cluster_mode,
                {GlideClusterClientConfiguration.PubSubChannelModes.Exact: {channel}},
                {GlideClientConfiguration.PubSubChannelModes.Exact: {channel}},
                callback=callback,
                context=context,
            )
            listening_client, publishing_client = await create_two_clients_with_pubsub(
                request, cluster_mode, pub_sub
            )

            # Verify subscription is active
            await wait_for_subscription_state(
                listening_client, expected_channels={channel}
            )

            result = await listening_client.subscribe([channel])
            assert result == OK

            # Verify subscription is still active (idempotent)
            await wait_for_subscription_state(
                listening_client, expected_channels={channel}
            )

            # Publish and verify message received (should only receive once)
            await publishing_client.publish(message, channel)
            await anyio.sleep(1)
            pubsub_msg = await get_message_by_method(
                method, listening_client, callback_messages, 0
            )
            assert pubsub_msg.message == message

            await check_no_messages_left(method, listening_client, callback_messages, 1)

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    async def test_unsubscribe_from_non_subscribed_channel(
        self, request, cluster_mode: bool
    ):
        """
        Test unsubscribing from a channel that's not subscribed (should not error).
        """
        listening_client = None
        try:
            channel = get_random_string(10)

            # Create client without subscriptions
            listening_client = await create_client(request, cluster_mode)

            # Verify no subscriptions
            await wait_for_subscription_state(listening_client, expected_channels=set())

            result = await listening_client.unsubscribe([channel])
            assert result == OK

            await wait_for_subscription_state(listening_client, expected_channels=set())

        finally:
            await client_cleanup(listening_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    async def test_subscribe_with_empty_list(self, request, cluster_mode: bool):
        """
        Test subscribing with an empty channel list.
        """
        listening_client = None
        try:
            # Create client without subscriptions
            listening_client = await create_client(request, cluster_mode)

            await wait_for_subscription_state(listening_client, expected_channels=set())

            result = await listening_client.subscribe([])
            assert result == OK

            await wait_for_subscription_state(listening_client, expected_channels=set())

        finally:
            await client_cleanup(listening_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_subscribe_with_bytes_and_strings(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Test subscribing with mix of bytes and string channel names.
        """
        listening_client, publishing_client = None, None
        try:
            channel1 = get_random_string(10)
            channel2_bytes = get_random_string(10).encode()
            message1 = get_random_string(5)
            message2 = get_random_string(5)

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            # Create clients
            listening_client = await create_client(
                request,
                cluster_mode,
                cluster_mode_pubsub=(
                    GlideClusterClientConfiguration.PubSubSubscriptions(
                        channels_and_patterns={},
                        callback=callback,
                        context=context,
                    )
                    if cluster_mode and callback
                    else None
                ),
                standalone_mode_pubsub=(
                    GlideClientConfiguration.PubSubSubscriptions(
                        channels_and_patterns={},
                        callback=callback,
                        context=context,
                    )
                    if not cluster_mode and callback
                    else None
                ),
            )
            publishing_client = await create_client(request, cluster_mode)

            # Subscribe with mixed types
            result = await listening_client.subscribe([channel1, channel2_bytes])
            assert result == OK

            # Verify both subscriptions are active
            # Note: get_active_subscriptions normalizes to strings
            await wait_for_subscription_state(
                listening_client,
                expected_channels={
                    channel1,
                    channel2_bytes.decode(),
                },
            )

            await publishing_client.publish(message1, channel1)
            await publishing_client.publish(message2, channel2_bytes)
            await anyio.sleep(1)

            received_channels = set()
            for index in range(2):
                pubsub_msg = await get_message_by_method(
                    method, listening_client, callback_messages, index
                )
                received_channels.add(pubsub_msg.channel)

            assert len(received_channels) == 2

            await check_no_messages_left(method, listening_client, callback_messages, 2)

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.skip_if_version_below("7.0.0")
    @pytest.mark.parametrize("cluster_mode", [True])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_mixed_exact_pattern_and_sharded(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Test operations with all three subscription types:
        exact, pattern, and sharded.
        """
        listening_client, publishing_client = None, None
        try:
            exact_channel = get_random_string(10)
            pattern = "news.*"
            pattern_channel = "news.sports"
            sharded_channel = get_random_string(10)
            message_exact = get_random_string(5)
            message_pattern = get_random_string(5)
            message_sharded = get_random_string(5)

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            listening_client = await create_client(
                request,
                cluster_mode,
                cluster_mode_pubsub=(
                    GlideClusterClientConfiguration.PubSubSubscriptions(
                        channels_and_patterns={},
                        callback=callback,
                        context=context,
                    )
                    if callback
                    else None
                ),
            )
            publishing_client = await create_client(request, cluster_mode)

            await listening_client.subscribe([exact_channel])
            await listening_client.psubscribe([pattern])
            await cast(GlideClusterClient, listening_client).ssubscribe(
                [sharded_channel]
            )

            # Verify all subscriptions are active
            await wait_for_subscription_state(
                listening_client,
                expected_channels={exact_channel},
                expected_patterns={pattern},
                expected_sharded={sharded_channel},
            )

            await publishing_client.publish(message_exact, exact_channel)
            await publishing_client.publish(message_pattern, pattern_channel)
            await cast(GlideClusterClient, publishing_client).publish(
                message_sharded, sharded_channel, sharded=True
            )
            await anyio.sleep(1)

            received = {}
            for index in range(3):
                pubsub_msg = await get_message_by_method(
                    method, listening_client, callback_messages, index
                )
                received[pubsub_msg.channel] = pubsub_msg.message

            assert exact_channel in received
            assert pattern_channel in received
            assert sharded_channel in received

            await check_no_messages_left(method, listening_client, callback_messages, 3)

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    async def test_rapid_subscribe_unsubscribe(self, request, cluster_mode: bool):
        """
        Test rapid subscribe/unsubscribe operations on the same channel.
        """
        listening_client = None
        try:
            channel = get_random_string(10)

            listening_client = await create_client(request, cluster_mode)

            # Rapidly subscribe and unsubscribe
            for _ in range(10):
                result = await listening_client.subscribe([channel])
                assert result == OK

                result = await listening_client.unsubscribe([channel])
                assert result == OK

            # Final subscribe and verify
            result = await listening_client.subscribe([channel])
            assert result == OK
            await wait_for_subscription_state(
                listening_client, expected_channels={channel}
            )

        finally:
            await client_cleanup(listening_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    @pytest.mark.parametrize(
        "method", [MethodTesting.Async, MethodTesting.Sync, MethodTesting.Callback]
    )
    async def test_partial_unsubscribe(
        self, request, cluster_mode: bool, method: MethodTesting
    ):
        """
        Test unsubscribing from a subset of subscribed channels.
        """
        listening_client, publishing_client = None, None
        try:
            channels = [get_random_string(10) for _ in range(3)]
            messages = {ch: get_random_string(5) for ch in channels}

            callback, context = None, None
            callback_messages: List[PubSubMsg] = []
            if method == MethodTesting.Callback:
                callback = new_message
                context = callback_messages

            # Create clients
            listening_client = await create_client(
                request,
                cluster_mode,
                cluster_mode_pubsub=(
                    GlideClusterClientConfiguration.PubSubSubscriptions(
                        channels_and_patterns={},
                        callback=callback,
                        context=context,
                    )
                    if cluster_mode and callback
                    else None
                ),
                standalone_mode_pubsub=(
                    GlideClientConfiguration.PubSubSubscriptions(
                        channels_and_patterns={},
                        callback=callback,
                        context=context,
                    )
                    if not cluster_mode and callback
                    else None
                ),
            )
            publishing_client = await create_client(request, cluster_mode)

            # Subscribe to all channels
            await listening_client.subscribe(channels)
            await wait_for_subscription_state(
                listening_client, expected_channels=set(channels)
            )

            # Unsubscribe from first channel only
            await listening_client.unsubscribe([channels[0]])
            await wait_for_subscription_state(
                listening_client, expected_channels={channels[1], channels[2]}
            )

            # Publish to all channels
            for channel, message in messages.items():
                await publishing_client.publish(message, channel)
            await anyio.sleep(1)

            # Should receive messages from channels[1] and channels[2] only
            received_channels = set()
            for index in range(2):
                pubsub_msg = await get_message_by_method(
                    method, listening_client, callback_messages, index
                )
                received_channels.add(pubsub_msg.channel)

            assert channels[0] not in received_channels
            assert channels[1] in received_channels
            assert channels[2] in received_channels

            await check_no_messages_left(method, listening_client, callback_messages, 2)

        finally:
            await client_cleanup(listening_client)
            await client_cleanup(publishing_client)

    @pytest.mark.parametrize("cluster_mode", [True, False])
    async def test_get_active_subscriptions_empty(self, request, cluster_mode: bool):
        """
        Test get_active_subscriptions() with no subscriptions returns empty sets.
        """
        client = None
        try:
            # Create client without subscriptions
            client = await create_client(request, cluster_mode)

            active = await wait_for_subscription_state(
                client,
                expected_channels=set(),
                expected_patterns=set(),
            )
            assert "channels" in active
            assert "patterns" in active

        finally:
            await client_cleanup(client)
