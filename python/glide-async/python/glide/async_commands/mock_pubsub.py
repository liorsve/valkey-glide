# Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

"""
Mock PubSub implementation for testing dynamic PubSub functionality.

This module provides a global PubSub broker that simulates Redis pub/sub behavior,
allowing multiple clients to communicate with each other through pub/sub channels.

TODO: Remove this entire module when Rust core implementation is ready.
"""

import fnmatch
import random
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

    def _initialize(self, max_application_delay: float = 1.0) -> None:
        """
        Initialize broker state.
        
        Args:
            max_application_delay: Maximum delay in seconds for applying subscription changes.
                                  Actual delay will be random between 0 and this value.
                                  Simulates the delay in bw desired and actual subscriptions.
        """
        self._subscribers_lock = threading.Lock()
        self._max_application_delay = max_application_delay

        # Desired subscriptions (updated immediately by API calls)
        self._desired_channel_subscribers: DefaultDict[str, Set[str]] = defaultdict(set)
        self._desired_pattern_subscribers: DefaultDict[str, Set[str]] = defaultdict(set)
        self._desired_sharded_subscribers: DefaultDict[str, Set[str]] = defaultdict(set)

        # Actual subscriptions (updated after delay to simulate server application)
        self._actual_channel_subscribers: DefaultDict[str, Set[str]] = defaultdict(set)
        self._actual_pattern_subscribers: DefaultDict[str, Set[str]] = defaultdict(set)
        self._actual_sharded_subscribers: DefaultDict[str, Set[str]] = defaultdict(set)

        # Client message queues: client_id -> queue of PubSubMsg
        self._client_queues: DefaultDict[str, List[PubSubMsg]] = defaultdict(list)

        # Client callbacks: client_id -> (callback, context)
        self._client_callbacks: Dict[str, Tuple[Optional[Callable], Any]] = {}
        
        # Track if client is cluster mode: client_id -> bool
        self._client_is_cluster: Dict[str, bool] = {}
        
        # Track pending timers for cleanup
        self._pending_timers: List[threading.Timer] = []

    def set_max_application_delay(self, max_delay: float):
        """Set the maximum application delay for subscription changes."""
        with self._subscribers_lock:
            self._max_application_delay = max_delay

    def register_client(
        self,
        client_id: str,
        callback: Optional[Callable[[PubSubMsg, Any], None]] = None,
        context: Any = None,
        is_cluster: bool = False,
    ):
        """Register a client with the broker."""
        with self._subscribers_lock:
            self._client_callbacks[client_id] = (callback, context)
            self._client_is_cluster[client_id] = is_cluster

    def unregister_client(self, client_id: str):
        """Unregister a client and clean up all its subscriptions."""
        with self._subscribers_lock:
            # Remove from all desired subscriptions
            for subscribers in self._desired_channel_subscribers.values():
                subscribers.discard(client_id)
            for subscribers in self._desired_pattern_subscribers.values():
                subscribers.discard(client_id)
            for subscribers in self._desired_sharded_subscribers.values():
                subscribers.discard(client_id)

            # Remove from all actual subscriptions
            for subscribers in self._actual_channel_subscribers.values():
                subscribers.discard(client_id)
            for subscribers in self._actual_pattern_subscribers.values():
                subscribers.discard(client_id)
            for subscribers in self._actual_sharded_subscribers.values():
                subscribers.discard(client_id)

            # Clean up client data
            self._client_queues.pop(client_id, None)
            self._client_callbacks.pop(client_id, None)
            self._client_is_cluster.pop(client_id, None)

    def _schedule_actual_update(
        self, 
        update_func: Callable[[], None]
    ):
        """Schedule an actual subscription update after a random delay."""
        delay = random.uniform(0, self._max_application_delay)
        timer = threading.Timer(delay, update_func)
        self._pending_timers.append(timer)
        timer.start()

    def _apply_channel_subscribe(self, client_id: str, channels: List[str]):
        """Apply actual channel subscription."""
        with self._subscribers_lock:
            for channel in channels:
                self._actual_channel_subscribers[channel].add(client_id)

    def _apply_pattern_subscribe(self, client_id: str, patterns: List[str]):
        """Apply actual pattern subscription."""
        with self._subscribers_lock:
            for pattern in patterns:
                self._actual_pattern_subscribers[pattern].add(client_id)

    def _apply_sharded_subscribe(self, client_id: str, channels: List[str]):
        """Apply actual sharded subscription."""
        with self._subscribers_lock:
            for channel in channels:
                self._actual_sharded_subscribers[channel].add(client_id)

    def _apply_channel_unsubscribe(self, client_id: str, channels: Optional[List[str]]):
        """Apply actual channel unsubscription."""
        with self._subscribers_lock:
            if channels is None:
                for subscribers in self._actual_channel_subscribers.values():
                    subscribers.discard(client_id)
            else:
                for channel in channels:
                    self._actual_channel_subscribers[channel].discard(client_id)

    def _apply_pattern_unsubscribe(self, client_id: str, patterns: Optional[List[str]]):
        """Apply actual pattern unsubscription."""
        with self._subscribers_lock:
            if patterns is None:
                for subscribers in self._actual_pattern_subscribers.values():
                    subscribers.discard(client_id)
            else:
                for pattern in patterns:
                    self._actual_pattern_subscribers[pattern].discard(client_id)

    def _apply_sharded_unsubscribe(self, client_id: str, channels: Optional[List[str]]):
        """Apply actual sharded unsubscription."""
        with self._subscribers_lock:
            if channels is None:
                for subscribers in self._actual_sharded_subscribers.values():
                    subscribers.discard(client_id)
            else:
                for channel in channels:
                    self._actual_sharded_subscribers[channel].discard(client_id)

    def _is_state_synchronized(self, client_id: str) -> bool:
        """Check if desired and actual states match for a client."""
        desired_channels = {
            channel
            for channel, subs in self._desired_channel_subscribers.items()
            if client_id in subs
        }
        actual_channels = {
            channel
            for channel, subs in self._actual_channel_subscribers.items()
            if client_id in subs
        }
        
        desired_patterns = {
            pattern
            for pattern, subs in self._desired_pattern_subscribers.items()
            if client_id in subs
        }
        actual_patterns = {
            pattern
            for pattern, subs in self._actual_pattern_subscribers.items()
            if client_id in subs
        }
        
        desired_sharded = {
            channel
            for channel, subs in self._desired_sharded_subscribers.items()
            if client_id in subs
        }
        actual_sharded = {
            channel
            for channel, subs in self._actual_sharded_subscribers.items()
            if client_id in subs
        }
        
        return (
            desired_channels == actual_channels
            and desired_patterns == actual_patterns
            and desired_sharded == actual_sharded
        )

    def subscribe(self, client_id: str, channels: List[str]) -> SubscriptionStatus:
        """Subscribe a client to exact channels."""
        with self._subscribers_lock:
            # Update desired state immediately
            for channel in channels:
                self._desired_channel_subscribers[channel].add(client_id)
            
            # Check if already in sync (no delay needed)
            is_synced = self._is_state_synchronized(client_id)
        
        # Schedule actual update outside the lock
        if not is_synced:
            self._schedule_actual_update(
                lambda: self._apply_channel_subscribe(client_id, channels)
            )
            return SubscriptionStatus.PENDING
        
        return SubscriptionStatus.OK

    def psubscribe(self, client_id: str, patterns: List[str]) -> SubscriptionStatus:
        """Subscribe a client to channel patterns."""
        with self._subscribers_lock:
            # Update desired state immediately
            for pattern in patterns:
                self._desired_pattern_subscribers[pattern].add(client_id)
            
            # Check if already in sync
            is_synced = self._is_state_synchronized(client_id)
        
        # Schedule actual update outside the lock
        if not is_synced:
            self._schedule_actual_update(
                lambda: self._apply_pattern_subscribe(client_id, patterns)
            )
            return SubscriptionStatus.PENDING
        
        return SubscriptionStatus.OK

    def unsubscribe(self, client_id: str, channels: Optional[List[str]] = None) -> SubscriptionStatus:
        """Unsubscribe a client from exact channels."""
        with self._subscribers_lock:
            # Update desired state immediately
            if channels is None:
                for subscribers in self._desired_channel_subscribers.values():
                    subscribers.discard(client_id)
            else:
                for channel in channels:
                    self._desired_channel_subscribers[channel].discard(client_id)
            
            # Check if already in sync
            is_synced = self._is_state_synchronized(client_id)
        
        # Schedule actual update outside the lock
        if not is_synced:
            self._schedule_actual_update(
                lambda: self._apply_channel_unsubscribe(client_id, channels)
            )
            return SubscriptionStatus.PENDING
        
        return SubscriptionStatus.OK

    def punsubscribe(self, client_id: str, patterns: Optional[List[str]] = None) -> SubscriptionStatus:
        """Unsubscribe a client from channel patterns."""
        with self._subscribers_lock:
            # Update desired state immediately
            if patterns is None:
                for subscribers in self._desired_pattern_subscribers.values():
                    subscribers.discard(client_id)
            else:
                for pattern in patterns:
                    self._desired_pattern_subscribers[pattern].discard(client_id)
            
            # Check if already in sync
            is_synced = self._is_state_synchronized(client_id)
        
        # Schedule actual update outside the lock
        if not is_synced:
            self._schedule_actual_update(
                lambda: self._apply_pattern_unsubscribe(client_id, patterns)
            )
            return SubscriptionStatus.PENDING
        
        return SubscriptionStatus.OK

    def ssubscribe(self, client_id: str, channels: List[str]) -> SubscriptionStatus:
        """Subscribe a client to sharded channels."""
        with self._subscribers_lock:
            # Update desired state immediately
            for channel in channels:
                self._desired_sharded_subscribers[channel].add(client_id)
            
            # Check if already in sync
            is_synced = self._is_state_synchronized(client_id)
        
        # Schedule actual update outside the lock
        if not is_synced:
            self._schedule_actual_update(
                lambda: self._apply_sharded_subscribe(client_id, channels)
            )
            return SubscriptionStatus.PENDING
        
        return SubscriptionStatus.OK

    def sunsubscribe(self, client_id: str, channels: Optional[List[str]] = None) -> SubscriptionStatus:
        """Unsubscribe a client from sharded channels."""
        with self._subscribers_lock:
            # Update desired state immediately
            if channels is None:
                for subscribers in self._desired_sharded_subscribers.values():
                    subscribers.discard(client_id)
            else:
                for channel in channels:
                    self._desired_sharded_subscribers[channel].discard(client_id)
            
            # Check if already in sync
            is_synced = self._is_state_synchronized(client_id)
        
        # Schedule actual update outside the lock
        if not is_synced:
            self._schedule_actual_update(
                lambda: self._apply_sharded_unsubscribe(client_id, channels)
            )
            return SubscriptionStatus.PENDING
        
        return SubscriptionStatus.OK
                    
    def publish(self, channel: str, message: str, sharded: bool = False) -> int:
        """
        Publish a message to a channel.
        Returns the number of clients that received the message.
        Uses actual subscriptions (not desired) to determine recipients.
        """
        with self._subscribers_lock:
            recipient_count = 0

            # Convert to bytes to match real Redis behavior
            channel_bytes = channel.encode() if isinstance(channel, str) else channel
            message_bytes = message.encode() if isinstance(message, str) else message

            if sharded:
                # Sharded publish - only exact channel matches from actual subscriptions
                subscribers = self._actual_sharded_subscribers.get(channel, set())
                for client_id in subscribers:
                    self._deliver_message(
                        client_id,
                        PubSubMsg(
                            message=message_bytes,
                            channel=channel_bytes,
                            pattern=None,
                        ),
                    )
                    recipient_count += 1
            else:
                # Regular publish - exact channel matches from actual subscriptions
                subscribers = self._actual_channel_subscribers.get(channel, set())
                for client_id in subscribers:
                    self._deliver_message(
                        client_id,
                        PubSubMsg(
                            message=message_bytes,
                            channel=channel_bytes,
                            pattern=None,
                        ),
                    )
                    recipient_count += 1

                # Pattern matches from actual subscriptions
                for pattern, pattern_subscribers in self._actual_pattern_subscribers.items():
                    if fnmatch.fnmatch(channel, pattern):
                        pattern_bytes = (
                            pattern.encode() if isinstance(pattern, str) else pattern
                        )
                        for client_id in pattern_subscribers:
                            self._deliver_message(
                                client_id,
                                PubSubMsg(
                                    message=message_bytes,
                                    channel=channel_bytes,
                                    pattern=pattern_bytes,
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

    def get_client_subscriptions(
        self, client_id: str
    ) -> Tuple[
        Dict[Any, Set[str]],  # desired
        Dict[Any, Set[str]],  # actual
    ]:
        """
        Get both desired and actual subscriptions for a specific client.
        Returns dictionaries with enum keys (PubSubChannelModes).
        
        Desired reflects what the client wants to be subscribed to.
        Actual reflects what is currently active on the server.
        """
        from glide_shared.config import (
            GlideClientConfiguration,
            GlideClusterClientConfiguration,
        )
        
        # Determine if this is a cluster client
        is_cluster = self._client_is_cluster.get(client_id, False)
        
        if is_cluster:
            PubSubChannelModes = GlideClusterClientConfiguration.PubSubChannelModes
        else:
            PubSubChannelModes = GlideClientConfiguration.PubSubChannelModes
        
        with self._subscribers_lock:
            # Get desired channels as strings
            desired_channels = {
                channel
                for channel, subs in self._desired_channel_subscribers.items()
                if client_id in subs
            }
            desired_patterns = {
                pattern
                for pattern, subs in self._desired_pattern_subscribers.items()
                if client_id in subs
            }
            desired_sharded = {
                channel
                for channel, subs in self._desired_sharded_subscribers.items()
                if client_id in subs
            }
            
            # Get actual channels as strings
            actual_channels = {
                channel
                for channel, subs in self._actual_channel_subscribers.items()
                if client_id in subs
            }
            actual_patterns = {
                pattern
                for pattern, subs in self._actual_pattern_subscribers.items()
                if client_id in subs
            }
            actual_sharded = {
                channel
                for channel, subs in self._actual_sharded_subscribers.items()
                if client_id in subs
            }

            # Build desired result with enum keys
            desired_result = {
                PubSubChannelModes.Exact: desired_channels,
                PubSubChannelModes.Pattern: desired_patterns,
            }
            
            # Build actual result with enum keys
            actual_result = {
                PubSubChannelModes.Exact: actual_channels,
                PubSubChannelModes.Pattern: actual_patterns,
            }
            
            # Add Sharded only for cluster mode
            if is_cluster:
                desired_result[PubSubChannelModes.Sharded] = desired_sharded
                actual_result[PubSubChannelModes.Sharded] = actual_sharded
            
            return desired_result, actual_result

    @classmethod
    def reset(cls):
        """Reset the broker (useful for testing)."""
        with cls._lock:
            if cls._instance is not None:
                # Cancel all pending timers
                for timer in cls._instance._pending_timers:
                    timer.cancel()
                cls._instance._pending_timers.clear()
                cls._instance._initialize()


def normalize_args(args: List[TEncodable]) -> List[str]:
    """Convert args to strings for internal storage."""
    return [arg.decode() if isinstance(arg, bytes) else str(arg) for arg in args]