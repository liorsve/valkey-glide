# Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

"""Integration tests validating PubSub code snippets from the docs website."""

import uuid
from typing import Any, List

import anyio
import pytest
from glide.glide_client import GlideClient
from glide_shared.config import GlideClientConfiguration, NodeAddress


def unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


async def make_client(**kwargs) -> GlideClient:
    config = GlideClientConfiguration(
        [NodeAddress("localhost", 6379)],
        **kwargs,
    )
    return await GlideClient.create(config)


@pytest.mark.anyio
async def test_publish():
    client = await make_client()
    try:
        result = await client.publish("Test message", unique("ch"))
        assert isinstance(result, int)
    finally:
        await client.close()


@pytest.mark.anyio
async def test_dynamic_subscribe_polling():
    ch = unique("poll")
    sub = await make_client()
    pub = await make_client()
    try:
        await sub.subscribe_lazy({ch})
        await anyio.sleep(1.5)

        await pub.publish("hello-poll", ch)
        msg = await sub.get_pubsub_message()
        assert msg is not None
        # message may be bytes or str
        assert b"hello-poll" in (
            msg.message if isinstance(msg.message, bytes) else msg.message.encode()
        )
    finally:
        await sub.close()
        await pub.close()


@pytest.mark.anyio
async def test_dynamic_subscribe_blocking():
    ch = unique("block")
    sub = await make_client()
    pub = await make_client()
    try:
        await sub.subscribe({ch}, timeout_ms=1000)
        await pub.publish("hello-block", ch)
        msg = await sub.get_pubsub_message()
        assert msg is not None
    finally:
        await sub.close()
        await pub.close()


@pytest.mark.anyio
async def test_pattern_subscribe():
    prefix = unique("pat")
    pattern = f"{prefix}*"
    channel = f"{prefix}-news"
    sub = await make_client()
    pub = await make_client()
    try:
        await sub.psubscribe_lazy({pattern})
        await anyio.sleep(1.5)

        await pub.publish("pattern-msg", channel)
        msg = await sub.get_pubsub_message()
        assert msg is not None
    finally:
        await sub.close()
        await pub.close()


@pytest.mark.anyio
async def test_callback_based():
    ch = unique("cb")
    received: List[Any] = []

    def callback(msg, context):
        received.append(msg.message)

    sub = await make_client(
        pubsub_subscriptions=GlideClientConfiguration.PubSubSubscriptions(
            channels_and_patterns={},
            callback=callback,
            context=None,
        ),
    )
    pub = await make_client()
    try:
        await sub.subscribe({ch}, timeout_ms=1000)
        await pub.publish("cb-hello", ch)
        await anyio.sleep(1.5)

        assert len(received) > 0, "Callback should have received at least one message"
    finally:
        await sub.close()
        await pub.close()


@pytest.mark.anyio
async def test_unsubscribe():
    ch = unique("unsub")
    client = await make_client()
    try:
        await client.subscribe({ch}, timeout_ms=1000)
        await client.unsubscribe_lazy({ch})
        await anyio.sleep(1)

        state = await client.get_subscriptions()
        assert state is not None
    finally:
        await client.close()


@pytest.mark.anyio
async def test_state_introspection():
    ch1, ch2 = unique("intr1"), unique("intr2")
    client = await make_client()
    try:
        await client.subscribe({ch1, ch2}, timeout_ms=1000)
        state = await client.get_subscriptions()
        assert state is not None
        assert hasattr(state, "desired_subscriptions")
    finally:
        await client.close()
