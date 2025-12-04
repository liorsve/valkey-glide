use async_trait::async_trait;
use redis::{Cmd, PushInfo, RedisResult, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc; 
use tokio::sync::mpsc;
pub use redis::pubsub_synchronizer::{PubSubSynchronizer, SubscriptionType};

// Keep the mock implementation
#[cfg(feature = "mock-pubsub")]
mod mock;

#[cfg(feature = "mock-pubsub")]
pub use mock::MockPubSubBroker;

// Keep the real implementation
#[cfg(not(feature = "mock-pubsub"))]
mod real;

/// Factory function to create a synchronizer
pub fn create_pubsub_synchronizer(
    cluster_mode: bool,
    push_sender: Option<mpsc::UnboundedSender<PushInfo>>,
    initial_subscriptions: Option<redis::PubSubSubscriptionInfo>,
) -> Arc<dyn PubSubSynchronizer> {
    #[cfg(feature = "mock-pubsub")]
    {
        mock::MockPubSubSynchronizer::new(cluster_mode, push_sender, initial_subscriptions)
    }
    
    #[cfg(not(feature = "mock-pubsub"))]
    {
        real::RealPubSubSynchronizer::new(cluster_mode, push_sender, initial_subscriptions)
    }
}