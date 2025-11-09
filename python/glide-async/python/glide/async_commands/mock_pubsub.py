# Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

"""
Mock PubSub implementation for testing dynamic PubSub functionality.

This module provides a global PubSub broker that simulates Redis pub/sub behavior,
allowing multiple clients to communicate with each other through pub/sub channels.

TODO: Remove this entire module when Rust core implementation is ready.
"""

import fnmatch
import threading
from collections import defaultdict
from typing import Any, Callable, DefaultDict, Dict, List, Optional, Set, Tuple

from glide_shared.commands.core_options import PubSubMsg, SubscriptionStatus
from glide_shared.constants import TEncodable
from glide.logger import Level as LogLevel
from glide.logger import Logger as ClientLogger


class MockPubSubBroker:
    """
    Global mock pubsub broker that simulates Redis pub/sub behavior.
    Allows multiple clients to communicate with each other through pub/sub.
    This is a singleton - all clients share the same broker instance.
    """

    _instance: Optional["MockPubSubBroker"] = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialize()
        return cls._instance

    def _initialize(self) -> None:
        """Initialize broker state."""
        self._subscribers_lock = threading.Lock()

        # Channel subscriptions: channel_name -> set of client_ids
        self._channel_subscribers: DefaultDict[str, Set[str]] = defaultdict(set)

        # Pattern subscriptions: pattern -> set of client_ids
        self._pattern_subscribers: DefaultDict[str, Set[str]] = defaultdict(set)

        # Sharded channel subscriptions: channel_name -> set of client_ids
        self._sharded_subscribers: DefaultDict[str, Set[str]] = defaultdict(set)

        # Client message queues: client_id -> queue of PubSubMsg
        self._client_queues: DefaultDict[str, List[PubSubMsg]] = defaultdict(list)

        # Client callbacks: client_id -> (callback, context)
        self._client_callbacks: Dict[str, Tuple[Optional[Callable], Any]] = {}

    def register_client(
        self,
        client_id: str,
        callback: Optional[Callable[[PubSubMsg, Any], None]] = None,
        context: Any = None,
    ):
        """Register a client with the broker."""
        with self._subscribers_lock:
            self._client_callbacks[client_id] = (callback, context)

    def unregister_client(self, client_id: str):
        """Unregister a client and clean up all its subscriptions."""
        with self._subscribers_lock:
            # Remove from all channel subscriptions
            for subscribers in self._channel_subscribers.values():
                subscribers.discard(client_id)

            # Remove from all pattern subscriptions
            for subscribers in self._pattern_subscribers.values():
                subscribers.discard(client_id)

            # Remove from all sharded subscriptions
            for subscribers in self._sharded_subscribers.values():
                subscribers.discard(client_id)

            # Clean up client data
            self._client_queues.pop(client_id, None)
            self._client_callbacks.pop(client_id, None)

    def subscribe(self, client_id: str, channels: List[str]) -> SubscriptionStatus:
        """Subscribe a client to exact channels."""
        with self._subscribers_lock:
            for channel in channels:
                self._channel_subscribers[channel].add(client_id)
        return SubscriptionStatus.OK

    def psubscribe(self, client_id: str, patterns: List[str]) -> SubscriptionStatus:
        """Subscribe a client to channel patterns."""
        with self._subscribers_lock:
            for pattern in patterns:
                self._pattern_subscribers[pattern].add(client_id)
        return SubscriptionStatus.OK

    def unsubscribe(self, client_id: str, channels: Optional[List[str]] = None) -> SubscriptionStatus:
        """Unsubscribe a client from exact channels."""
        with self._subscribers_lock:
            if channels is None:
                for subscribers in self._channel_subscribers.values():
                    subscribers.discard(client_id)
            else:
                for channel in channels:
                    self._channel_subscribers[channel].discard(client_id)
        return SubscriptionStatus.OK

    def punsubscribe(self, client_id: str, patterns: Optional[List[str]] = None) -> SubscriptionStatus:
        """Unsubscribe a client from channel patterns."""
        with self._subscribers_lock:
            if patterns is None:
                for subscribers in self._pattern_subscribers.values():
                    subscribers.discard(client_id)
            else:
                for pattern in patterns:
                    self._pattern_subscribers[pattern].discard(client_id)
        return SubscriptionStatus.OK

    def ssubscribe(self, client_id: str, channels: List[str]) -> SubscriptionStatus:
        """Subscribe a client to sharded channels."""
        with self._subscribers_lock:
            for channel in channels:
                self._sharded_subscribers[channel].add(client_id)
        return SubscriptionStatus.OK

    def sunsubscribe(self, client_id: str, channels: Optional[List[str]] = None) -> SubscriptionStatus:
        """Unsubscribe a client from sharded channels."""
        with self._subscribers_lock:
            if channels is None:
                for subscribers in self._sharded_subscribers.values():
                    subscribers.discard(client_id)
            else:
                for channel in channels:
                    self._sharded_subscribers[channel].discard(client_id)
        return SubscriptionStatus.OK
                    
    def publish(self, channel: str, message: str, sharded: bool = False) -> int:
        """
        Publish a message to a channel.
        Returns the number of clients that received the message.
        """
        with self._subscribers_lock:
            recipient_count = 0

            # Convert to bytes to match real Redis behavior
            channel_bytes = channel.encode() if isinstance(channel, str) else channel
            message_bytes = message.encode() if isinstance(message, str) else message

            if sharded:
                # Sharded publish - only exact channel matches
                subscribers = self._sharded_subscribers.get(channel, set())
                for client_id in subscribers:
                    self._deliver_message(
                        client_id,
                        PubSubMsg(
                            message=message_bytes,  # ← bytes
                            channel=channel_bytes,  # ← bytes
                            pattern=None,
                        ),
                    )
                    recipient_count += 1
            else:
                # Regular publish - exact channel matches
                subscribers = self._channel_subscribers.get(channel, set())
                for client_id in subscribers:
                    self._deliver_message(
                        client_id,
                        PubSubMsg(
                            message=message_bytes,  # ← bytes
                            channel=channel_bytes,  # ← bytes
                            pattern=None,
                        ),
                    )
                    recipient_count += 1

                # Pattern matches
                for pattern, pattern_subscribers in self._pattern_subscribers.items():
                    if fnmatch.fnmatch(channel, pattern):
                        pattern_bytes = (
                            pattern.encode() if isinstance(pattern, str) else pattern
                        )
                        for client_id in pattern_subscribers:
                            self._deliver_message(
                                client_id,
                                PubSubMsg(
                                    message=message_bytes,  # ← bytes
                                    channel=channel_bytes,  # ← bytes
                                    pattern=pattern_bytes,  # ← bytes
                                ),
                            )
                            recipient_count += 1

            return recipient_count

    def _deliver_message(self, client_id: str, msg: PubSubMsg):
        """Deliver a message to a specific client."""
        callback, context = self._client_callbacks.get(client_id, (None, None))

        if callback is not None:
            # If client has a callback, invoke it
            try:
                callback(msg, context)
            except Exception as e:
                ClientLogger.log(
                    LogLevel.WARN,
                    "pubsub callback error",
                    f"Error in pubsub callback for client {client_id}: {e}",
                )
        else:
            # Otherwise, queue the message for later retrieval
            self._client_queues[client_id].append(msg)

    def get_client_message(self, client_id: str) -> Optional[PubSubMsg]:
        """Get the next queued message for a client, if any."""
        with self._subscribers_lock:
            queue = self._client_queues.get(client_id, [])
            if queue:
                return queue.pop(0)
            return None

    def get_client_subscriptions(self, client_id: str) -> Dict[str, Set[bytes]]:
        """Get all subscriptions for a specific client."""
        with self._subscribers_lock:
            channels = {
                channel.encode()  # ← Convert to bytes
                for channel, subs in self._channel_subscribers.items()
                if client_id in subs
            }
            patterns = {
                pattern.encode()  # ← Convert to bytes
                for pattern, subs in self._pattern_subscribers.items()
                if client_id in subs
            }
            sharded = {
                channel.encode()  # ← Convert to bytes
                for channel, subs in self._sharded_subscribers.items()
                if client_id in subs
            }

            return {
                "channels": channels,
                "patterns": patterns,
                "sharded_channels": sharded,
            }

    @classmethod
    def reset(cls):
        """Reset the broker (useful for testing)."""
        with cls._lock:
            if cls._instance is not None:
                cls._instance._initialize()


def normalize_args(args: List[TEncodable]) -> List[str]:
    """Convert args to strings for internal storage."""
    return [arg.decode() if isinstance(arg, bytes) else str(arg) for arg in args]
