// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

use async_trait::async_trait;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SubscriptionType {
    Exact,
    Pattern,
    Sharded,
}

/// Trait for managing PubSub subscription synchronization between desired and actual state.
#[async_trait]
pub trait PubSubSynchronizer: Send + Sync {
    /// Add channels to desired subscriptions
    async fn add_desired_subscriptions(
        &self,
        channels: HashSet<String>,
        subscription_type: SubscriptionType,
    );

    /// Remove channels from desired subscriptions
    /// If channels is None, remove all subscriptions of this type
    async fn remove_desired_subscriptions(
        &self,
        channels: Option<HashSet<String>>,
        subscription_type: SubscriptionType,
    );

    /// Add channels to current (actual) subscriptions
    async fn add_current_subscriptions(
        &self,
        channels: HashSet<String>,
        subscription_type: SubscriptionType,
    );

    /// Remove channels from current (actual) subscriptions
    /// If channels is empty, remove all subscriptions of this type
    async fn remove_current_subscriptions(
        &self,
        channels: HashSet<String>,
        subscription_type: SubscriptionType,
    );

    /// Get the current state of both desired and actual subscriptions
    async fn get_subscription_state(
        &self,
    ) -> (
        HashMap<String, HashSet<String>>,
        HashMap<String, HashSet<String>>,
    );

    /// Reconcile desired and actual subscriptions
    async fn reconcile(&self) -> Result<(), String>;

    /// Check if desired and actual subscriptions are synchronized
    async fn is_synchronized(&self) -> bool {
        let (desired, actual) = self.get_subscription_state().await;
        desired == actual
    }

    /// Remove all current subscriptions associated with a specific address
    /// This is called when a node disconnects, so reconciliation can restore them
    /// Default implementation: does nothing (for implementations that don't track by address)
    async fn remove_current_subscriptions_for_address(&self, _address: &str) {
        // Default: no-op
    }

    /// Try to intercept and handle a pubsub command
    /// Returns Some(result) if the command was handled, None if it should go through normal path
    /// Default implementation: don't intercept, let commands go through normal path
    async fn intercept_pubsub_command(&self, _cmd: &crate::Cmd) -> Option<crate::RedisResult<crate::Value>> {
        // Default: don't intercept
        None
    }

    /// Set initial subscriptions from client configuration and trigger immediate reconciliation
    /// Default implementation: add to desired and reconcile
    async fn set_initial_subscriptions(
        &self,
        channels: HashSet<String>,
        patterns: HashSet<String>,
        sharded: HashSet<String>,
    ) {
        if !channels.is_empty() {
            self.add_desired_subscriptions(channels, SubscriptionType::Exact).await;
        }
        if !patterns.is_empty() {
            self.add_desired_subscriptions(patterns, SubscriptionType::Pattern).await;
        }
        if !sharded.is_empty() {
            self.add_desired_subscriptions(sharded, SubscriptionType::Sharded).await;
        }
        let _ = self.reconcile().await;
    }
}