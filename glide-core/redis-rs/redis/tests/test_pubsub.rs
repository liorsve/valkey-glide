#![allow(unknown_lints, dependency_on_unit_never_type_fallback)]
#![cfg(feature = "cluster-async")]

mod support;

#[cfg(test)]
mod cluster_async_pubsub {
    use std::{
        collections::HashSet,
        path::PathBuf,
        sync::OnceLock,
        time::Duration,
    };

    use futures_time::task::sleep;
    use telemetrylib::*;
    use tokio::runtime::Runtime;

    use redis::{
        cluster_routing::{Route, RoutingInfo, SingleNodeRoutingInfo, SlotAddr},
        cluster_topology::get_slot,
        cmd, ProtocolVersion, PubSubChannelOrPattern,
        PubSubSubscriptionInfo, PubSubSubscriptionKind, PushInfo, PushKind, RedisError,
        Value,
    };

    use crate::support::*;
    use tokio::sync::mpsc;

    const METRICS_JSON: &str = "/tmp/metrics.json";
    const PUBLISH_TIME: u64 = 2000;

    fn shared_runtime() -> &'static Runtime {
        static RUNTIME: OnceLock<Runtime> = OnceLock::new();
        RUNTIME.get_or_init(|| Runtime::new().expect("Failed to create runtime"))
    }

    async fn init_otel() -> Result<(), GlideOTELError> {
        let config = GlideOpenTelemetryConfigBuilder::default()
            .with_flush_interval(Duration::from_millis(PUBLISH_TIME))
            .with_metrics_exporter(GlideOpenTelemetrySignalsExporter::File(PathBuf::from(
                METRICS_JSON,
            )))
            .build();
        if let Err(e) = GlideOpenTelemetry::initialise(config) {
            panic!("Failed to initialize OpenTelemetry: {e}");
        }
        Ok(())
    }

    fn read_latest_metrics_json() -> serde_json::Value {
        let file_content =
            std::fs::read_to_string(METRICS_JSON).expect("Failed to read metrics JSON file");
        let lines: Vec<&str> = file_content
            .lines()
            .filter(|l| !l.trim().is_empty())
            .collect();
        serde_json::from_str(lines.last().expect("No metrics lines found"))
            .expect("Failed to parse metrics JSON")
    }

    fn find_metric<'a>(
        metrics_json: &'a serde_json::Value,
        metric_name: &str,
    ) -> &'a serde_json::Value {
        metrics_json["scope_metrics"][0]["metrics"]
            .as_array()
            .expect("Expected 'metrics' to be an array")
            .iter()
            .find(|m| m["name"] == metric_name)
            .unwrap_or_else(|| panic!("Metric '{metric_name}' not found"))
    }

    fn get_start_value(metric_name: &str) -> u64 {
        let file_content = match std::fs::read_to_string(METRICS_JSON) {
            Ok(content) => content,
            Err(_) => return 0, // File not found or unreadable
        };

        let lines: Vec<&str> = file_content
            .split('\n')
            .filter(|l| !l.trim().is_empty())
            .collect();

        if lines.is_empty() {
            return 0;
        }

        let metric_json: serde_json::Value = match serde_json::from_str(lines.last().unwrap()) {
            Ok(json) => json,
            Err(_) => return 0, // Invalid JSON
        };

        let metric = match metric_json["scope_metrics"][0]["metrics"]
            .as_array()
            .and_then(|metrics| metrics.iter().find(|m| m["name"] == metric_name))
        {
            Some(m) => m,
            None => return 0,
        };

        metric["data_points"][0]["value"].as_u64().unwrap_or(0)
    }

    #[derive(Debug, Clone, Copy)]
    enum PublishCommand {
        Publish,
        SPublish,
    }

    async fn retry_publish_until_expected_subscribers(
        command: PublishCommand,
        connection: &mut redis::cluster_async::ClusterConnection,
        channel: &str,
        message: &str,
        expected_count: i64,
        max_retries: u32,
    ) -> redis::RedisResult<redis::Value> {
        let mut delay_ms = 100u64;

        eprintln!("🔁 [Retry Publish] Starting with expected_count={}, max_retries={}", 
                expected_count, max_retries);

        for attempt in 0..max_retries {
            eprintln!("🔁 [Retry Publish] Attempt {}/{}", attempt + 1, max_retries);
            
            let cmd_name = match command {
                PublishCommand::Publish => "PUBLISH",
                PublishCommand::SPublish => "SPUBLISH",
            };

            let result = redis::cmd(cmd_name)
                .arg(channel)
                .arg(message)
                .query_async(connection)
                .await;

            eprintln!("🔁 [Retry Publish] Result: {:?}", result);

            match result {
                Ok(redis::Value::Int(count)) => {
                    eprintln!("🔁 [Retry Publish] Got count={}, expected={}", count, expected_count);
                    
                    if count == expected_count {
                        eprintln!("✅ [Retry Publish] SUCCESS! Count matches expected");
                        return Ok(redis::Value::Int(count));
                    } else {
                        eprintln!("⚠️ [Retry Publish] Count mismatch: got {}, expected {}", 
                                count, expected_count);
                        
                        if attempt == max_retries - 1 {
                            eprintln!("❌ [Retry Publish] Max retries reached, returning last result");
                            return Ok(redis::Value::Int(count));
                        }
                        
                        eprintln!("⏳ [Retry Publish] Sleeping {}ms before retry", delay_ms);
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                        delay_ms = std::cmp::min(delay_ms * 2, 5000);
                    }
                }
                Ok(other) => {
                    eprintln!("⚠️ [Retry Publish] Unexpected response type: {:?}", other);
                    return Ok(other);
                }
                Err(e) => {
                    eprintln!("❌ [Retry Publish] Error: {:?}", e);
                    return Err(e);
                }
            }
        }

        eprintln!("❌ [Retry Publish] Loop completed without return (SHOULD NOT HAPPEN!)");
        unreachable!("Loop should exit via return")
    }

    fn validate_subscriptions(
        pubsub_subs: &PubSubSubscriptionInfo,
        notifications_rx: &mut mpsc::UnboundedReceiver<PushInfo>,
        allow_disconnects: bool,
    ) {
        let mut subscribe_cnt =
            if let Some(exact_subs) = pubsub_subs.get(&PubSubSubscriptionKind::Exact) {
                exact_subs.len()
            } else {
                0
            };

        let mut psubscribe_cnt =
            if let Some(pattern_subs) = pubsub_subs.get(&PubSubSubscriptionKind::Pattern) {
                pattern_subs.len()
            } else {
                0
            };

        let mut ssubscribe_cnt =
            if let Some(sharded_subs) = pubsub_subs.get(&PubSubSubscriptionKind::Sharded) {
                sharded_subs.len()
            } else {
                0
            };

        for _ in 0..(subscribe_cnt + psubscribe_cnt + ssubscribe_cnt) {
            let result = notifications_rx.try_recv();
            assert!(result.is_ok());
            let PushInfo { kind, data: _ } = result.unwrap();
            assert!(
                kind == PushKind::Subscribe
                    || kind == PushKind::PSubscribe
                    || kind == PushKind::SSubscribe
                    || if allow_disconnects {
                        kind == PushKind::Disconnection
                    } else {
                        false
                    }
            );
            if kind == PushKind::Subscribe {
                subscribe_cnt -= 1;
            } else if kind == PushKind::PSubscribe {
                psubscribe_cnt -= 1;
            } else if kind == PushKind::SSubscribe {
                ssubscribe_cnt -= 1;
            }
        }

        assert!(subscribe_cnt == 0);
        assert!(psubscribe_cnt == 0);
        assert!(ssubscribe_cnt == 0);
    }

    // ============================================================================
    // Dynamic PubSub Subscription State Management Tests
    // ============================================================================

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_subscribe_happy_path() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
            let mut connection = cluster.async_connection(Some(tx)).await;
            let mut publish_connection = cluster.async_connection(None).await;
            
            let channels = vec![
                PubSubChannelOrPattern::from("test_channel_1".as_bytes()),
                PubSubChannelOrPattern::from("test_channel_2".as_bytes()),
            ];
            
            let result = connection.subscribe(channels.clone()).await;
            assert!(result.is_ok());
            
            let (desired, current) = connection.get_subscriptions().await.unwrap();
            
            // Check desired state
            let desired_channels = desired.get("channels").unwrap();
            assert!(desired_channels.contains(&channels[0]));
            assert!(desired_channels.contains(&channels[1]));
            
            // Check current state (actually subscribed)
            let current_channels = current.get("channels").unwrap();
            assert!(current_channels.contains(&channels[0]));
            assert!(current_channels.contains(&channels[1]));
            
            // Verify actually works by publishing
            let result = retry_publish_until_expected_subscribers(
                PublishCommand::Publish,
                &mut publish_connection,
                "test_channel_1",
                "test_msg",
                1,
                10,
            )
            .await;
            assert_eq!(result, Ok(Value::Int(1)));
            
            // Drain subscription notifications
            while let Ok(push) = rx.try_recv() {
                if push.kind == PushKind::Message {
                    assert_eq!(
                        push.data,
                        vec![
                            Value::BulkString("test_channel_1".into()),
                            Value::BulkString("test_msg".into()),
                        ]
                    );
                    break;
                }
            }
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_unsubscribe_happy_path() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            let mut publish_connection = cluster.async_connection(None).await;
            
            let channel = PubSubChannelOrPattern::from("test_channel".as_bytes());
            
            // Subscribe first
            let _ = connection.subscribe(vec![channel.clone()]).await;
            
            // Verify subscribed
            let (_, current) = connection.get_subscriptions().await.unwrap();
            assert!(current.get("channels").unwrap().contains(&channel));
            
            // Then unsubscribe
            let result = connection.unsubscribe(Some(vec![channel.clone()])).await;
            assert!(result.is_ok());
                        
            let (desired, current) = connection.get_subscriptions().await.unwrap();
            
            // Check both desired and current don't have the channel
            let empty_set = HashSet::new();
            let desired_channels = desired.get("channels").unwrap_or(&empty_set);
            assert!(!desired_channels.contains(&channel));
            
            let current_channels = current.get("channels").unwrap_or(&empty_set);
            assert!(!current_channels.contains(&channel));
            
            // Verify actually unsubscribed by publishing
            let result = cmd("PUBLISH")
                .arg("test_channel")
                .arg("should_not_receive")
                .query_async::<_, Value>(&mut publish_connection)
                .await
                .unwrap();
            
            // Should return 0 subscribers
            assert_eq!(result, Value::Int(0));
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_psubscribe_happy_path() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
            let mut connection = cluster.async_connection(Some(tx)).await;
            let mut publish_connection = cluster.async_connection(None).await;
            
            let pattern = PubSubChannelOrPattern::from("test_*".as_bytes());
            
            let result = connection.psubscribe(vec![pattern.clone()]).await;
            assert!(result.is_ok());
                        
            let (desired, current) = connection.get_subscriptions().await.unwrap();
            
            // Check desired state
            let desired_patterns = desired.get("patterns").unwrap();
            assert!(desired_patterns.contains(&pattern));
            
            // Check current state (actually subscribed)
            let current_patterns = current.get("patterns").unwrap();
            assert!(current_patterns.contains(&pattern));
            
            // Verify pattern matching works
            let _ = cmd("PUBLISH")
                .arg("test_channel_123")
                .arg("pattern_msg")
                .query_async::<_, Value>(&mut publish_connection)
                .await;
            
            sleep(Duration::from_millis(200).into()).await;
            
            // Should receive pattern message
            loop {
                let push = rx.try_recv().unwrap();
                if push.kind == PushKind::PMessage {
                    assert_eq!(push.data[2], Value::BulkString("pattern_msg".into()));
                    break;
                }
            }
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_punsubscribe_happy_path() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
            let mut connection = cluster.async_connection(Some(tx)).await;
            let mut publish_connection = cluster.async_connection(None).await;
            
            let pattern = PubSubChannelOrPattern::from("test_*".as_bytes());
            
            // Subscribe first
            let _ = connection.psubscribe(vec![pattern.clone()]).await;
            
            // Verify subscribed by receiving a message
            let _ = cmd("PUBLISH")
                .arg("test_matching_channel")
                .arg("before_unsub")
                .query_async::<_, Value>(&mut publish_connection)
                .await
                .unwrap();
            
            sleep(Duration::from_millis(200).into()).await;
            
            // Drain until we find the message
            let mut found_message = false;
            while let Ok(push) = rx.try_recv() {
                if push.kind == PushKind::PMessage {
                    found_message = true;
                    assert_eq!(
                        push.data,
                        vec![
                            Value::BulkString("test_*".into()),
                            Value::BulkString("test_matching_channel".into()),
                            Value::BulkString("before_unsub".into()),
                        ]
                    );
                    break;
                }
            }
            assert!(found_message, "Should have received pattern message before unsubscribe");
            
            // Then unsubscribe
            let result = connection.punsubscribe(Some(vec![pattern.clone()])).await;
            assert!(result.is_ok());
            
            
            let (desired, current) = connection.get_subscriptions().await.unwrap();
            
            let empty_set = HashSet::new();
            let desired_patterns = desired.get("patterns").unwrap_or(&empty_set);
            assert!(!desired_patterns.contains(&pattern));
            
            let current_patterns = current.get("patterns").unwrap_or(&empty_set);
            assert!(!current_patterns.contains(&pattern));
            
            let _ = cmd("PUBLISH")
                .arg("test_matching_channel")
                .arg("should_not_receive")
                .query_async::<_, Value>(&mut publish_connection)
                .await
                .unwrap();
            
            sleep(Duration::from_millis(200).into()).await;
            
            // 🔥 Check the receiver - should NOT receive a PMessage
            match rx.try_recv() {
                Ok(push) => {
                    // Only unsubscribe-related pushes are acceptable
                    assert!(
                        push.kind != PushKind::PMessage,
                        "Should NOT receive PMessage after punsubscribe, got: {:?}",
                        push
                    );
                }
                Err(mpsc::error::TryRecvError::Empty) => {
                    // This is the expected result
                }
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_ssubscribe_happy_path() {
        block_on_all(async move {
            if engine_version_less_than("7.0").await {
                return Ok::<_, RedisError>(());
            }

            let cluster = TestClusterContext::new_with_cluster_client_builder(
                3,
                0,
                |builder| builder.use_protocol(ProtocolVersion::RESP3),
                false,
            );

            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
            let mut connection = cluster.async_connection(Some(tx)).await;
            let mut publish_connection = cluster.async_connection(None).await;
            
            let channel = PubSubChannelOrPattern::from("test_sharded".as_bytes());
            
            let result = connection.ssubscribe(vec![channel.clone()]).await;
            assert!(result.is_ok());
                        
            let (desired, current) = connection.get_subscriptions().await.unwrap();
            
            // Check desired state
            let desired_sharded = desired.get("sharded_channels").unwrap();
            assert!(desired_sharded.contains(&channel));
            
            // Check current state (actually subscribed)
            let current_sharded = current.get("sharded_channels").unwrap();
            assert!(current_sharded.contains(&channel));
            
            // Verify actually works by publishing
            let result = retry_publish_until_expected_subscribers(
                PublishCommand::SPublish,
                &mut publish_connection,
                "test_sharded",
                "sharded_msg",
                1,
                10,
            )
            .await;
            assert_eq!(result, Ok(Value::Int(1)));
            
            sleep(Duration::from_millis(200).into()).await;
            
            // Should receive sharded message
            loop {
                let push = rx.try_recv().unwrap();
                if push.kind == PushKind::SMessage {
                    assert_eq!(
                        push.data,
                        vec![
                            Value::BulkString("test_sharded".into()),
                            Value::BulkString("sharded_msg".into()),
                        ]
                    );
                    break;
                }
            }
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_sunsubscribe_happy_path() {
        block_on_all(async move {
            if engine_version_less_than("7.0").await {
                return Ok::<_, RedisError>(());
            }

            let cluster = TestClusterContext::new_with_cluster_client_builder(
                3,
                0,
                |builder| builder.use_protocol(ProtocolVersion::RESP3),
                false,
            );

            let mut connection = cluster.async_connection(None).await;
            let mut publish_connection = cluster.async_connection(None).await;
            
            let channel = PubSubChannelOrPattern::from("test_sharded".as_bytes());
            
            // Subscribe first
            let _ = connection.ssubscribe(vec![channel.clone()]).await;
            
            // Verify subscribed
            let (_, current) = connection.get_subscriptions().await.unwrap();
            assert!(current.get("sharded_channels").unwrap().contains(&channel));
            
            // Then unsubscribe
            let result = connection.sunsubscribe(Some(vec![channel.clone()])).await;
            assert!(result.is_ok());
                        
            let (desired, current) = connection.get_subscriptions().await.unwrap();
            
            let empty_set = HashSet::new();
            let desired_sharded = desired.get("sharded_channels").unwrap_or(&empty_set);
            assert!(!desired_sharded.contains(&channel));
            
            let current_sharded = current.get("sharded_channels").unwrap_or(&empty_set);
            assert!(!current_sharded.contains(&channel));
            
            // Verify no messages received
            let result = cmd("SPUBLISH")
                .arg("test_sharded")
                .arg("should_not_receive")
                .query_async::<_, Value>(&mut publish_connection)
                .await
                .unwrap();
            
            // Should return 0 subscribers
            assert_eq!(result, Value::Int(0));
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_subscribe_lazy_happy_path() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
            let mut connection = cluster.async_connection(Some(tx)).await;
            let mut publish_connection = cluster.async_connection(None).await;
            
            let channel = PubSubChannelOrPattern::from("lazy_test".as_bytes());
            
            // Lazy subscribe
            let result = connection.subscribe_lazy(vec![channel.clone()]).await;
            assert!(result.is_ok());
            
            // Desired state updated immediately
            let (desired, _) = connection.get_subscriptions().await.unwrap();
            assert!(desired.get("channels").unwrap().contains(&channel));
            
            // Wait for reconciliation
            sleep(Duration::from_millis(300).into()).await;
            
            // Current state eventually matches
            let (_, current) = connection.get_subscriptions().await.unwrap();
            assert!(current.get("channels").unwrap().contains(&channel));
            
            // Verify actually subscribed by publishing
            let result = retry_publish_until_expected_subscribers(
                PublishCommand::Publish,
                &mut publish_connection,
                "lazy_test",
                "test_msg",
                1,
                10,
            )
            .await;
            assert_eq!(result, Ok(Value::Int(1)));
            
            sleep(Duration::from_millis(200).into()).await;
            
            // Should receive message
            loop {
                let push = rx.try_recv().unwrap();
                if push.kind == PushKind::Message {
                    assert_eq!(
                        push.data,
                        vec![
                            Value::BulkString("lazy_test".into()),
                            Value::BulkString("test_msg".into()),
                        ]
                    );
                    break;
                }
            }
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_unsubscribe_lazy_happy_path() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            let mut publish_connection = cluster.async_connection(None).await;
            
            let channel = PubSubChannelOrPattern::from("lazy_unsub_test".as_bytes());
            
            // Subscribe first (blocking to ensure established)
            let _ = connection.subscribe(vec![channel.clone()]).await;
            sleep(Duration::from_millis(300).into()).await;
            
            // Verify subscribed
            let (_, current) = connection.get_subscriptions().await.unwrap();
            assert!(current.get("channels").unwrap().contains(&channel));
            
            // Lazy unsubscribe
            let result = connection.unsubscribe_lazy(Some(vec![channel.clone()])).await;
            assert!(result.is_ok());
            
            // Desired state updated immediately
            let (desired, _) = connection.get_subscriptions().await.unwrap();
            let empty_set = HashSet::new();
            assert!(!desired.get("channels").unwrap_or(&empty_set).contains(&channel));
            
            // Wait for reconciliation
            sleep(Duration::from_millis(300).into()).await;
            
            // Current state eventually matches
            let (_, current) = connection.get_subscriptions().await.unwrap();
            assert!(!current.get("channels").unwrap_or(&empty_set).contains(&channel));
            
            // Verify actually unsubscribed
            let result = cmd("PUBLISH")
                .arg("lazy_unsub_test")
                .arg("should_not_receive")
                .query_async::<_, Value>(&mut publish_connection)
                .await
                .unwrap();
            
            assert_eq!(result, Value::Int(0));
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_psubscribe_lazy_happy_path() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
            let mut connection = cluster.async_connection(Some(tx)).await;
            let mut publish_connection = cluster.async_connection(None).await;
            
            let pattern = PubSubChannelOrPattern::from("lazy_pattern_*".as_bytes());
            
            // Lazy psubscribe
            let result = connection.psubscribe_lazy(vec![pattern.clone()]).await;
            assert!(result.is_ok());
            
            // Desired state updated immediately
            let (desired, _) = connection.get_subscriptions().await.unwrap();
            assert!(desired.get("patterns").unwrap().contains(&pattern));
            
            // Wait for reconciliation
            sleep(Duration::from_millis(300).into()).await;
            
            // Current state eventually matches
            let (_, current) = connection.get_subscriptions().await.unwrap();
            assert!(current.get("patterns").unwrap().contains(&pattern));
            
            // Verify pattern matching works
            let _ = cmd("PUBLISH")
                .arg("lazy_pattern_test")
                .arg("lazy_msg")
                .query_async::<_, Value>(&mut publish_connection)
                .await;
            
            sleep(Duration::from_millis(200).into()).await;
            
            // Should receive pattern message
            loop {
                let push = rx.try_recv().unwrap();
                if push.kind == PushKind::PMessage {
                    assert_eq!(push.data[2], Value::BulkString("lazy_msg".into()));
                    break;
                }
            }
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_punsubscribe_lazy_happy_path() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            let mut publish_connection = cluster.async_connection(None).await;
            
            let pattern = PubSubChannelOrPattern::from("lazy_pattern_*".as_bytes());
            
            // Subscribe first
            let _ = connection.psubscribe(vec![pattern.clone()]).await;
            sleep(Duration::from_millis(300).into()).await;
            
            // Verify subscribed
            let (_, current) = connection.get_subscriptions().await.unwrap();
            assert!(current.get("patterns").unwrap().contains(&pattern));
            
            // Lazy unsubscribe
            let result = connection.punsubscribe_lazy(Some(vec![pattern.clone()])).await;
            assert!(result.is_ok());
            
            // Desired state updated immediately
            let (desired, _) = connection.get_subscriptions().await.unwrap();
            let empty_set = HashSet::new();
            assert!(!desired.get("patterns").unwrap_or(&empty_set).contains(&pattern));
            
            // Wait for reconciliation
            sleep(Duration::from_millis(300).into()).await;
            
            // Current state eventually matches
            let (_, current) = connection.get_subscriptions().await.unwrap();
            assert!(!current.get("patterns").unwrap_or(&empty_set).contains(&pattern));
            
            // Verify no messages received
            let result = cmd("PUBLISH")
                .arg("lazy_pattern_test")
                .arg("should_not_receive")
                .query_async::<_, Value>(&mut publish_connection)
                .await
                .unwrap();
            
            assert_eq!(result, Value::Int(0));
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_ssubscribe_lazy_happy_path() {
        block_on_all(async move {
            if engine_version_less_than("7.0").await {
                return Ok::<_, RedisError>(());
            }

            let cluster = TestClusterContext::new_with_cluster_client_builder(
                3,
                0,
                |builder| builder.use_protocol(ProtocolVersion::RESP3),
                false,
            );

            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
            let mut connection = cluster.async_connection(Some(tx)).await;
            let mut publish_connection = cluster.async_connection(None).await;
            
            let channel = PubSubChannelOrPattern::from("lazy_sharded".as_bytes());
            
            // Lazy ssubscribe
            let result = connection.ssubscribe_lazy(vec![channel.clone()]).await;
            assert!(result.is_ok());
            
            // Desired state updated immediately
            let (desired, _) = connection.get_subscriptions().await.unwrap();
            assert!(desired.get("sharded_channels").unwrap().contains(&channel));
            
            // Wait for reconciliation
            sleep(Duration::from_millis(300).into()).await;
            
            // Current state eventually matches
            let (_, current) = connection.get_subscriptions().await.unwrap();
            assert!(current.get("sharded_channels").unwrap().contains(&channel));
            
            // Verify actually subscribed
            let result = retry_publish_until_expected_subscribers(
                PublishCommand::SPublish,
                &mut publish_connection,
                "lazy_sharded",
                "lazy_sharded_msg",
                1,
                10,
            )
            .await;
            assert_eq!(result, Ok(Value::Int(1)));
            
            sleep(Duration::from_millis(200).into()).await;
            
            // Should receive message
            loop {
                let push = rx.try_recv().unwrap();
                if push.kind == PushKind::SMessage {
                    assert_eq!(
                        push.data,
                        vec![
                            Value::BulkString("lazy_sharded".into()),
                            Value::BulkString("lazy_sharded_msg".into()),
                        ]
                    );
                    break;
                }
            }
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_sunsubscribe_lazy_happy_path() {
        block_on_all(async move {
            if engine_version_less_than("7.0").await {
                return Ok::<_, RedisError>(());
            }

            let cluster = TestClusterContext::new_with_cluster_client_builder(
                3,
                0,
                |builder| builder.use_protocol(ProtocolVersion::RESP3),
                false,
            );

            let mut connection = cluster.async_connection(None).await;
            let mut publish_connection = cluster.async_connection(None).await;
            
            let channel = PubSubChannelOrPattern::from("lazy_sharded_unsub".as_bytes());
            
            // Subscribe first
            let _ = connection.ssubscribe(vec![channel.clone()]).await;
            sleep(Duration::from_millis(300).into()).await;
            
            // Verify subscribed
            let (_, current) = connection.get_subscriptions().await.unwrap();
            assert!(current.get("sharded_channels").unwrap().contains(&channel));
            
            // Lazy unsubscribe
            let result = connection.sunsubscribe_lazy(Some(vec![channel.clone()])).await;
            assert!(result.is_ok());
            
            // Desired state updated immediately
            let (desired, _) = connection.get_subscriptions().await.unwrap();
            let empty_set = HashSet::new();
            assert!(!desired.get("sharded_channels").unwrap_or(&empty_set).contains(&channel));
            
            // Wait for reconciliation
            sleep(Duration::from_millis(300).into()).await;
            
            // Current state eventually matches
            let (_, current) = connection.get_subscriptions().await.unwrap();
            assert!(!current.get("sharded_channels").unwrap_or(&empty_set).contains(&channel));
            
            // Verify actually unsubscribed
            let result = cmd("SPUBLISH")
                .arg("lazy_sharded_unsub")
                .arg("should_not_receive")
                .query_async::<_, Value>(&mut publish_connection)
                .await
                .unwrap();
            
            assert_eq!(result, Value::Int(0));
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }


    #[test]
    #[serial_test::serial]
    fn test_async_cluster_unsubscribe_all_channels() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            let mut publish_connection = cluster.async_connection(None).await;
            
            let channels = vec![
                PubSubChannelOrPattern::from("ch1".as_bytes()),
                PubSubChannelOrPattern::from("ch2".as_bytes()),
                PubSubChannelOrPattern::from("ch3".as_bytes()),
            ];
            
            // Subscribe to multiple channels
            let _ = connection.subscribe(channels.clone()).await;
            sleep(Duration::from_millis(300).into()).await;
            
            // Verify subscribed by publishing
            let result = retry_publish_until_expected_subscribers(
                PublishCommand::Publish,
                &mut publish_connection,
                "ch1",
                "test",
                1,
                10,
            )
            .await;
            assert_eq!(result, Ok(Value::Int(1)));
            
            // Unsubscribe from all (None means all) - blocking mode
            let result = connection.unsubscribe(None).await;
            assert!(result.is_ok());
            
            sleep(Duration::from_millis(300).into()).await;
            
            let (desired, current) = connection.get_subscriptions().await.unwrap();
            
            let empty_set = HashSet::new();
            let desired_channels = desired.get("channels").unwrap_or(&empty_set);
            assert!(desired_channels.is_empty());
            
            let current_channels = current.get("channels").unwrap_or(&empty_set);
            assert!(current_channels.is_empty());
            
            // Verify actually unsubscribed by publishing
            let result = retry_publish_until_expected_subscribers(
                PublishCommand::Publish,
                &mut publish_connection,
                "ch1",
                "should_not_receive",
                0,
                10,
            )
            .await;
            assert_eq!(result, Ok(Value::Int(0)));
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }


    #[test]
    #[serial_test::serial]
    fn test_async_cluster_get_subscriptions_empty_when_no_subscriptions() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            
            let (desired, current) = connection.get_subscriptions().await.unwrap();
            
            // Check desired
            assert!(desired.get("channels").unwrap_or(&HashSet::new()).is_empty());
            assert!(desired.get("patterns").unwrap_or(&HashSet::new()).is_empty());
            assert!(desired.get("sharded_channels").unwrap_or(&HashSet::new()).is_empty());
            
            // Check current
            assert!(current.get("channels").unwrap_or(&HashSet::new()).is_empty());
            assert!(current.get("patterns").unwrap_or(&HashSet::new()).is_empty());
            assert!(current.get("sharded_channels").unwrap_or(&HashSet::new()).is_empty());
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_mixed_subscription_types() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            
            let channel = PubSubChannelOrPattern::from("exact_channel".as_bytes());
            let pattern = PubSubChannelOrPattern::from("pattern_*".as_bytes());
            
            // Subscribe to different types
            let _ = connection.subscribe(vec![channel.clone()]).await;
            let _ = connection.psubscribe(vec![pattern.clone()]).await;
            
            sleep(Duration::from_millis(200).into()).await;
            
            let (desired, current) = connection.get_subscriptions().await.unwrap();
            
            // Check desired
            assert!(desired.get("channels").unwrap().contains(&channel));
            assert!(desired.get("patterns").unwrap().contains(&pattern));
            
            // Check current
            assert!(current.get("channels").unwrap().contains(&channel));
            assert!(current.get("patterns").unwrap().contains(&pattern));
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_reconciliation_after_disconnect() {
        init_logger();
        let client_subscriptions = PubSubSubscriptionInfo::from([(
            PubSubSubscriptionKind::Exact,
            HashSet::from([PubSubChannelOrPattern::from("reconnect_test".as_bytes())]),
        )]);

        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| {
                builder
                    .retries(3)
                    .use_protocol(ProtocolVersion::RESP3)
                    .pubsub_subscriptions(client_subscriptions.clone())
                    .periodic_connections_checks(Some(Duration::from_secs(1)))
            },
            false,
        );

        block_on_all(async move {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
            let mut _listening_con = cluster.async_connection(Some(tx)).await;
            let mut publishing_con = cluster.async_connection(None).await;

            // short sleep to allow the server to push subscription notification
            sleep(futures_time::time::Duration::from_secs(1)).await;
            
            // validate subscriptions
            validate_subscriptions(&client_subscriptions, &mut rx, false);

            // validate PUBLISH - retry until expected subscribers are available
            let result = retry_publish_until_expected_subscribers(
                PublishCommand::Publish,
                &mut publishing_con,
                "reconnect_test",
                "before",
                2,
                10,
            )
            .await;
            assert_eq!(result, Ok(Value::Int(2)));
            
            sleep(futures_time::time::Duration::from_secs(1)).await;
            
            let result = rx.try_recv();
            assert!(result.is_ok());
            let PushInfo { kind, data } = result.unwrap();
            assert_eq!(
                (kind, data),
                (
                    PushKind::Message,
                    vec![
                        Value::BulkString("reconnect_test".into()),
                        Value::BulkString("before".into()),
                    ]
                )
            );

            // simulate passive disconnect
            drop(cluster);
            
            // recreate the cluster WITH THE SAME CONFIG (critical!)
            let _cluster = TestClusterContext::new_with_cluster_client_builder(
                3,
                0,
                |builder| {
                    builder
                        .retries(3)
                        .use_protocol(ProtocolVersion::RESP3)
                        .pubsub_subscriptions(client_subscriptions.clone())
                        .periodic_connections_checks(Some(Duration::from_secs(1)))
                },
                false,
            );

            // sleep for 1 periodic_connections_checks + overhead
            sleep(futures_time::time::Duration::from_secs(1 + 1)).await;

            // new subscription notifications due to resubscriptions
            validate_subscriptions(&client_subscriptions, &mut rx, true);

            // validate PUBLISH - retry until expected subscribers are available
            let result = retry_publish_until_expected_subscribers(
                PublishCommand::Publish,
                &mut publishing_con,
                "reconnect_test",
                "after",
                2,
                10,
            )
            .await;
            assert_eq!(result, Ok(Value::Int(2)));
            
            sleep(futures_time::time::Duration::from_secs(1)).await;
            
            let result = rx.try_recv();
            assert!(result.is_ok());
            let PushInfo { kind, data } = result.unwrap();
            assert_eq!(
                (kind, data),
                (
                    PushKind::Message,
                    vec![
                        Value::BulkString("reconnect_test".into()),
                        Value::BulkString("after".into()),
                    ]
                )
            );

            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_concurrent_subscribe_unsubscribe() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            
            let channel = PubSubChannelOrPattern::from("concurrent_test".as_bytes());
            
            // Rapidly subscribe and unsubscribe
            for _ in 0..5 {
                let _ = connection.subscribe(vec![channel.clone()]).await;
                let _ = connection.unsubscribe(Some(vec![channel.clone()])).await;
            }
            
            // Final subscribe
            let _ = connection.subscribe(vec![channel.clone()]).await;
            
            // Wait for reconciliation
            sleep(Duration::from_millis(300).into()).await;
            
            // Verify final state
            let (desired, current) = connection.get_subscriptions().await.unwrap();
            let desired_channels = desired.get("channels").unwrap();
            let current_channels = current.get("channels").unwrap();
            
            assert!(desired_channels.contains(&channel));
            assert!(current_channels.contains(&channel));
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_pubsub_desync_metric() {
        let rt = shared_runtime();
        rt.block_on(async {
            let _ = std::fs::remove_file(METRICS_JSON);
            init_otel().await.unwrap();

            sleep(Duration::from_millis(PUBLISH_TIME + 100).into()).await;
            let start_desync_value = get_start_value("glide.pubsub_out_of_sync");

            // Force a desync situation by recording the metric
            telemetrylib::GlideOpenTelemetry::record_pubsub_out_of_sync().unwrap();
            
            sleep(Duration::from_millis(PUBLISH_TIME + 100).into()).await;

            let metric_json = read_latest_metrics_json();
            let desync_metric = find_metric(&metric_json, "glide.pubsub_out_of_sync");

            assert_eq!(
                desync_metric["data_points"][0]["value"],
                1 + start_desync_value
            );
        });
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_subscribe_to_same_channel_twice() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            
            let channel = PubSubChannelOrPattern::from("duplicate_test".as_bytes());
            
            // Subscribe twice
            let _ = connection.subscribe(vec![channel.clone()]).await;
            let _ = connection.subscribe(vec![channel.clone()]).await;
            
            sleep(Duration::from_millis(200).into()).await;
            
            // Should only be subscribed once
            let (desired, current) = connection.get_subscriptions().await.unwrap();
            
            let desired_channels = desired.get("channels").unwrap();
            assert_eq!(desired_channels.len(), 1);
            assert!(desired_channels.contains(&channel));
            
            let current_channels = current.get("channels").unwrap();
            assert_eq!(current_channels.len(), 1);
            assert!(current_channels.contains(&channel));
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_unsubscribe_nonexistent_channel() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            
            let channel = PubSubChannelOrPattern::from("nonexistent".as_bytes());
            
            // Unsubscribe without subscribing first - should not error
            let result = connection.unsubscribe(Some(vec![channel])).await;
            assert!(result.is_ok());
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_subscription_survives_slot_migration() {
        init_logger();
        block_on_all(async move {
            if engine_version_less_than("7.0").await {
                return Ok::<_, RedisError>(());
            }

            let channel = "migrate_sub_test";
            let channel_slot = get_slot(channel.as_bytes());

            // Don't pass subscriptions to builder
            let cluster = TestClusterContext::new_with_cluster_client_builder(
                3,
                0,
                |builder| {
                    builder
                        .use_protocol(ProtocolVersion::RESP3)
                        .slots_refresh_rate_limit(Duration::from_secs(0), 0)
                },
                false,
            );

            let mut connection = cluster.async_connection(None).await;
            let mut publish_connection = cluster.async_connection(None).await;
            
            // Dynamically subscribe (only on connection, not publish_connection)
            let _ = connection.ssubscribe(vec![PubSubChannelOrPattern::from(channel.as_bytes())]).await;
            
            // Wait for subscription
            sleep(Duration::from_millis(300).into()).await;
            
            // Verify can receive message before migration
            let result = retry_publish_until_expected_subscribers(
                PublishCommand::SPublish,
                &mut publish_connection,
                channel,
                "before_migration",
                1,  // Now only 1 subscriber
                10,
            )
            .await;
            assert_eq!(result, Ok(Value::Int(1)));
            
            // Migrate slot
            let cluster_nodes = cluster.get_cluster_nodes().await;
            let slot_distribution = cluster.get_slots_ranges_distribution(&cluster_nodes);
            cluster.move_specific_slot(channel_slot, slot_distribution).await;
            
            // Wait for reconciliation
            sleep(Duration::from_millis(500).into()).await;
            
            // Verify still subscribed after migration
            let result = retry_publish_until_expected_subscribers(
                PublishCommand::SPublish,
                &mut publish_connection,
                channel,
                "after_migration",
                1,  // Still only 1 subscriber
                10,
            )
            .await;
            assert_eq!(result, Ok(Value::Int(1)));
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_restore_resp3_pubsub_state_passive_disconnect() {
        init_logger();
        block_on_all(async move {
            let redis_ver = std::env::var("REDIS_VERSION").unwrap_or_default();
            let use_sharded = redis_ver.starts_with("7.");

            let mut client_subscriptions = PubSubSubscriptionInfo::from([(
                PubSubSubscriptionKind::Exact,
                HashSet::from([PubSubChannelOrPattern::from("test_channel".as_bytes())]),
            )]);

            if use_sharded {
                client_subscriptions.insert(
                    PubSubSubscriptionKind::Sharded,
                    HashSet::from([PubSubChannelOrPattern::from("test_channel_?".as_bytes())]),
                );
            }

            // note topology change detection is not activated since no topology change is expected
            let cluster = TestClusterContext::new_with_cluster_client_builder(
                3,
                0,
                |builder| {
                    builder
                        .retries(3)
                        .use_protocol(ProtocolVersion::RESP3)
                        .pubsub_subscriptions(client_subscriptions.clone())
                        .periodic_connections_checks(Some(Duration::from_secs(1)))
                },
                false,
            );

            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
            let mut _listening_con = cluster.async_connection(Some(tx.clone())).await;
            // Note, publishing connection has the same pubsub config
            let mut publishing_con = cluster.async_connection(None).await;

            // short sleep to allow the server to push subscription notification
            sleep(futures_time::time::Duration::from_secs(1)).await;

            // validate subscriptions
            validate_subscriptions(&client_subscriptions, &mut rx, false);

            // validate PUBLISH - retry until expected subscribers are available
            let result = retry_publish_until_expected_subscribers(
                PublishCommand::Publish,
                &mut publishing_con,
                "test_channel",
                "test_message",
                2,
                10, // max retries
            )
            .await;
            assert_eq!(
                result,
                Ok(Value::Int(2)) // 2 connections with the same pubsub config
            );

            sleep(futures_time::time::Duration::from_secs(1)).await;
            let result = rx.try_recv();
            assert!(result.is_ok());
            let PushInfo { kind, data } = result.unwrap();
            assert_eq!(
                (kind, data),
                (
                    PushKind::Message,
                    vec![
                        Value::BulkString("test_channel".into()),
                        Value::BulkString("test_message".into()),
                    ]
                )
            );

            if use_sharded {
                // validate SPUBLISH - retry until expected subscribers are available
                let result = retry_publish_until_expected_subscribers(
                    PublishCommand::SPublish,
                    &mut publishing_con,
                    "test_channel_?",
                    "test_message",
                    2,
                    10, // max retries
                )
                .await;
                assert_eq!(
                    result,
                    Ok(Value::Int(2)) // 2 connections with the same pubsub config
                );

                sleep(futures_time::time::Duration::from_secs(1)).await;
                let result = rx.try_recv();
                assert!(result.is_ok());
                let PushInfo { kind, data } = result.unwrap();
                assert_eq!(
                    (kind, data),
                    (
                        PushKind::SMessage,
                        vec![
                            Value::BulkString("test_channel_?".into()),
                            Value::BulkString("test_message".into()),
                        ]
                    )
                );
            }

            // simulate passive disconnect
            drop(cluster);

            // recreate the cluster, the assumption is that the cluster is built with exactly the same params (ports, slots map...)
            let _cluster =
                TestClusterContext::new_with_cluster_client_builder(3, 0, |builder| builder, false);

            // sleep for 1 periodic_connections_checks + overhead
            sleep(Duration::from_secs(1 + 1).into()).await;


            // validate PUBLISH - retry until expected subscribers are available
            let result = retry_publish_until_expected_subscribers(
                PublishCommand::Publish,
                &mut publishing_con,
                "test_channel",
                "test_message",
                2,
                10, // max retries
            )
            .await;
            assert_eq!(
                result,
                Ok(Value::Int(2)) // 2 connections with the same pubsub config
            );

            // new subscription notifications due to resubscriptions
            validate_subscriptions(&client_subscriptions, &mut rx, true);

            sleep(futures_time::time::Duration::from_secs(1)).await;
            let result = rx.try_recv();
            assert!(result.is_ok());
            let PushInfo { kind, data } = result.unwrap();
            assert_eq!(
                (kind, data),
                (
                    PushKind::Message,
                    vec![
                        Value::BulkString("test_channel".into()),
                        Value::BulkString("test_message".into()),
                    ]
                )
            );

            if use_sharded {
                // validate SPUBLISH - retry until expected subscribers are available
                let result = retry_publish_until_expected_subscribers(
                    PublishCommand::SPublish,
                    &mut publishing_con,
                    "test_channel_?",
                    "test_message",
                    2,
                    10, // max retries
                )
                .await;
                assert_eq!(
                    result,
                    Ok(Value::Int(2)) // 2 connections with the same pubsub config
                );

                sleep(futures_time::time::Duration::from_secs(1)).await;
                let result = rx.try_recv();
                assert!(result.is_ok());
                let PushInfo { kind, data } = result.unwrap();
                assert_eq!(
                    (kind, data),
                    (
                        PushKind::SMessage,
                        vec![
                            Value::BulkString("test_channel_?".into()),
                            Value::BulkString("test_message".into()),
                        ]
                    )
                );
            }

            Ok(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_restore_resp3_pubsub_state_after_scale_out() {
        eprintln!("🚀 [TEST START] test_async_cluster_restore_resp3_pubsub_state_after_scale_out");
        
        let redis_ver = std::env::var("REDIS_VERSION").unwrap_or_default();
        let use_sharded = redis_ver.starts_with("7.");
        eprintln!("📌 [TEST] Redis version: {}, use_sharded: {}", redis_ver, use_sharded);

        let mut client_subscriptions = PubSubSubscriptionInfo::from([
            (
                PubSubSubscriptionKind::Exact,
                HashSet::from([PubSubChannelOrPattern::from("test_channel_?".as_bytes())]),
            ),
        ]);

        if use_sharded {
            client_subscriptions.insert(
                PubSubSubscriptionKind::Sharded,
                HashSet::from([PubSubChannelOrPattern::from("test_channel_?".as_bytes())]),
            );
        }

        let slot_14212 = get_slot(b"test_channel_?");
        assert_eq!(slot_14212, 14212);
        eprintln!("📌 [TEST] Slot for test_channel_?: {}", slot_14212);

        eprintln!("📌 [TEST] Creating initial 3-node cluster...");
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| {
                builder
                    .retries(3)
                    .use_protocol(ProtocolVersion::RESP3)
                    .pubsub_subscriptions(client_subscriptions.clone())
                    .periodic_connections_checks(Some(Duration::from_secs(1)))
                    .periodic_topology_checks(Duration::from_secs(1))
                    .slots_refresh_rate_limit(Duration::from_secs(0), 0)
            },
            false,
        );
        eprintln!("✅ [TEST] Initial cluster created");

        block_on_all(async move {
            eprintln!("📌 [TEST] Creating connections...");
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
            let mut _listening_con = cluster.async_connection(Some(tx.clone())).await;
            eprintln!("✅ [TEST] Listening connection created");
            
            let mut publishing_con = cluster.async_connection(None).await;
            eprintln!("✅ [TEST] Publishing connection created");

            eprintln!("📌 [TEST] Sleeping 1s for initial subscriptions...");
            sleep(futures_time::time::Duration::from_secs(1)).await;

            eprintln!("📌 [TEST] Validating initial subscriptions...");
            validate_subscriptions(&client_subscriptions, &mut rx, false);
            eprintln!("✅ [TEST] Initial subscriptions validated");

            eprintln!("📌 [TEST] First PUBLISH (before scale-out)...");
            let result = retry_publish_until_expected_subscribers(
                PublishCommand::Publish,
                &mut publishing_con,
                "test_channel_?",
                "test_message",
                2,
                10,
            )
            .await;
            eprintln!("✅ [TEST] First PUBLISH result: {:?}", result);
            assert_eq!(result, Ok(Value::Int(2)));

            eprintln!("📌 [TEST] Sleeping 1s...");
            sleep(futures_time::time::Duration::from_secs(1)).await;
            
            eprintln!("📌 [TEST] Receiving message from rx...");
            let result = rx.try_recv();
            assert!(result.is_ok());
            let PushInfo { kind, data } = result.unwrap();
            eprintln!("✅ [TEST] Received message: kind={:?}", kind);
            assert_eq!(
                (kind, data),
                (
                    PushKind::Message,
                    vec![
                        Value::BulkString("test_channel_?".into()),
                        Value::BulkString("test_message".into()),
                    ]
                )
            );

            if use_sharded {
                eprintln!("📌 [TEST] Testing SPUBLISH (before scale-out)...");
                let result = retry_publish_until_expected_subscribers(
                    PublishCommand::SPublish,
                    &mut publishing_con,
                    "test_channel_?",
                    "test_message",
                    2,
                    10,
                )
                .await;
                eprintln!("✅ [TEST] SPUBLISH result: {:?}", result);
                assert_eq!(result, Ok(Value::Int(2)));

                sleep(futures_time::time::Duration::from_secs(1)).await;
                let result = rx.try_recv();
                assert!(result.is_ok());
                let PushInfo { kind, data } = result.unwrap();
                eprintln!("✅ [TEST] Received sharded message: kind={:?}", kind);
                assert_eq!(
                    (kind, data),
                    (
                        PushKind::SMessage,
                        vec![
                            Value::BulkString("test_channel_?".into()),
                            Value::BulkString("test_message".into()),
                        ]
                    )
                );
            }

            // ========== SCALE OUT STARTS HERE ==========
            eprintln!("\n🔥 [TEST] ========== DROPPING OLD CLUSTER ==========");
            drop(cluster);
            eprintln!("✅ [TEST] Old cluster dropped");

            eprintln!("📌 [TEST] Creating new 6-node cluster...");
            let cluster = TestClusterContext::new_with_cluster_client_builder(
                6, 
                0, 
                |builder| builder, 
                false
            );
            eprintln!("✅ [TEST] New 6-node cluster created");

            eprintln!("📌 [TEST] Getting last server port...");
            let last_server_port = {
                let addr = cluster.cluster.servers.last().unwrap().addr.clone();
                match addr {
                    redis::ConnectionAddr::TcpTls {
                        host: _,
                        port,
                        insecure: _,
                        tls_params: _,
                    } => port,
                    redis::ConnectionAddr::Tcp(_, port) => port,
                    _ => {
                        panic!("Wrong server address type: {addr:?}");
                    }
                }
            };
            eprintln!("📌 [TEST] Last server port: {}", last_server_port);

            // ========== TOPOLOGY DISCOVERY LOOP ==========
            eprintln!("\n🔍 [TEST] ========== STARTING TOPOLOGY DISCOVERY ==========");
            let max_requests = 5;
            let mut i = 0;
            let mut cmd = redis::cmd("INFO");
            cmd.arg("SERVER");
            
            loop {
                eprintln!("🔍 [TEST] Topology discovery attempt {}/{}", i + 1, max_requests);
                
                if i == max_requests {
                    eprintln!("❌ [TEST] FAILED to discover new topology after {} attempts", max_requests);
                    panic!("Failed to recover and discover new topology");
                }
                i += 1;

                eprintln!("🔍 [TEST] Sending INFO SERVER to slot {}...", slot_14212);
                let route_result = publishing_con
                    .route_command(
                        &cmd,
                        RoutingInfo::SingleNode(SingleNodeRoutingInfo::SpecificNode(Route::new(
                            slot_14212,
                            SlotAddr::Master,
                        ))),
                    )
                    .await;
                
                eprintln!("🔍 [TEST] INFO SERVER result: {:?}", route_result);

                if let Ok(res) = route_result {
                    match res {
                        Value::VerbatimString { format: _, text } => {
                            let expected_port_str = format!("tcp_port:{}", last_server_port);
                            eprintln!("🔍 [TEST] Checking if response contains: {}", expected_port_str);
                            
                            if text.contains(&expected_port_str) {
                                eprintln!("✅ [TEST] NEW TOPOLOGY DISCOVERED! Slot {} now on port {}", 
                                        slot_14212, last_server_port);
                                break;
                            } else {
                                eprintln!("⏳ [TEST] Wrong node, response doesn't contain expected port");
                                eprintln!("⏳ [TEST] Response text (first 200 chars): {}", 
                                        &text.chars().take(200).collect::<String>());
                            }
                        }
                        _ => {
                            eprintln!("❌ [TEST] Wrong return type for INFO SERVER: {:?}", res);
                            panic!("Wrong return type for INFO SERVER command: {res:?}");
                        }
                    }
                } else {
                    eprintln!("⚠️ [TEST] INFO SERVER failed with error: {:?}", route_result);
                }
                
                eprintln!("⏳ [TEST] Sleeping 1s before retry...");
                sleep(futures_time::time::Duration::from_secs(1)).await;
            }
            eprintln!("✅ [TEST] Topology discovery complete!\n");

            eprintln!("📌 [TEST] Sleeping 1s for topology to settle...");
            sleep(futures_time::time::Duration::from_secs(1)).await;

            // ========== POST SCALE-OUT PUBLISH ==========
            eprintln!("\n🔥 [TEST] ========== STARTING POST SCALE-OUT PUBLISH ==========");
            eprintln!("📌 [TEST] About to call retry_publish_until_expected_subscribers...");
            
            let result = retry_publish_until_expected_subscribers(
                PublishCommand::Publish,
                &mut publishing_con,
                "test_channel_?",
                "test_message",
                2,
                10,
            )
            .await;
            
            eprintln!("✅ [TEST] retry_publish_until_expected_subscribers returned: {:?}", result);
            assert_eq!(result, Ok(Value::Int(2)));

            eprintln!("📌 [TEST] Sleeping 1s for message to propagate...");
            sleep(futures_time::time::Duration::from_secs(1)).await;

            eprintln!("📌 [TEST] Receiving messages from rx...");
            loop {
                let result = rx.try_recv();
                eprintln!("📌 [TEST] rx.try_recv() result: {:?}", 
                        result.as_ref().map(|p| &p.kind));
                
                assert!(result.is_ok());
                let PushInfo { kind, data } = result.unwrap();
                
                if kind == PushKind::Message {
                    eprintln!("✅ [TEST] Received Message push after scale-out");
                    assert_eq!(
                        data,
                        vec![
                            Value::BulkString("test_channel_?".into()),
                            Value::BulkString("test_message".into()),
                        ]
                    );
                    break;
                } else {
                    eprintln!("⏭️ [TEST] Ignoring push kind: {:?}", kind);
                }
            }

            if use_sharded {
                eprintln!("\n📌 [TEST] Testing SPUBLISH after scale-out...");
                let result = retry_publish_until_expected_subscribers(
                    PublishCommand::SPublish,
                    &mut publishing_con,
                    "test_channel_?",
                    "test_message",
                    2,
                    10,
                )
                .await;
                eprintln!("✅ [TEST] SPUBLISH result: {:?}", result);
                assert_eq!(result, Ok(Value::Int(2)));

                sleep(futures_time::time::Duration::from_secs(1)).await;
                let result = rx.try_recv();
                assert!(result.is_ok());
                let PushInfo { kind, data } = result.unwrap();
                eprintln!("✅ [TEST] Received sharded message after scale-out: kind={:?}", kind);
                assert_eq!(
                    (kind, data),
                    (
                        PushKind::SMessage,
                        vec![
                            Value::BulkString("test_channel_?".into()),
                            Value::BulkString("test_message".into()),
                        ]
                    )
                );
            }

            eprintln!("📌 [TEST] Dropping connections...");
            drop(publishing_con);
            drop(_listening_con);
            eprintln!("✅ [TEST] Connections dropped");

            eprintln!("✅ [TEST] Test completed successfully!");
            Ok(())
        })
        .unwrap();

        eprintln!("📌 [TEST] Final sleep 10s...");
        block_on_all(async move {
            sleep(futures_time::time::Duration::from_secs(10)).await;
            Ok(())
        })
        .unwrap();
        
        eprintln!("🎉 [TEST END] test_async_cluster_restore_resp3_pubsub_state_after_scale_out");
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_resp3_pubsub() {
        block_on_all(async move {
            let redis_ver = std::env::var("REDIS_VERSION").unwrap_or_default();
            let use_sharded = redis_ver.starts_with("7.");

            let mut client_subscriptions = PubSubSubscriptionInfo::from([
                (
                    PubSubSubscriptionKind::Exact,
                    HashSet::from([PubSubChannelOrPattern::from("test_channel_?".as_bytes())]),
                ),
                (
                    PubSubSubscriptionKind::Pattern,
                    HashSet::from([
                        PubSubChannelOrPattern::from("test_*".as_bytes()),
                        PubSubChannelOrPattern::from("*".as_bytes()),
                    ]),
                ),
            ]);

            if use_sharded {
                client_subscriptions.insert(
                    PubSubSubscriptionKind::Sharded,
                    HashSet::from([PubSubChannelOrPattern::from("test_channel_?".as_bytes())]),
                );
            }

            let cluster = TestClusterContext::new_with_cluster_client_builder(
                3,
                0,
                |builder| {
                    builder
                        .retries(3)
                        .use_protocol(ProtocolVersion::RESP3)
                        .pubsub_subscriptions(client_subscriptions.clone())
                },
                false,
            );

            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
            let mut connection = cluster.async_connection(Some(tx.clone())).await;

            // short sleep to allow the server to push subscription notification
            sleep(Duration::from_secs(1).into()).await;

            validate_subscriptions(&client_subscriptions, &mut rx, false);

            let slot_14212 = get_slot(b"test_channel_?");
            assert_eq!(slot_14212, 14212);

            let slot_0_route =
                redis::cluster_routing::Route::new(0, redis::cluster_routing::SlotAddr::Master);
            let node_0_route =
                redis::cluster_routing::SingleNodeRoutingInfo::SpecificNode(slot_0_route);

            // node 0 route is used to ensure that the publish is propagated correctly
            let result = connection
                .route_command(
                    redis::Cmd::new()
                        .arg("PUBLISH")
                        .arg("test_channel_?")
                        .arg("test_message"),
                    RoutingInfo::SingleNode(node_0_route.clone()),
                )
                .await;
            assert!(result.is_ok());

            sleep(Duration::from_secs(1).into()).await;

            let mut pmsg_cnt = 0;
            let mut msg_cnt = 0;
            for _ in 0..3 {
                let result = rx.try_recv();
                assert!(result.is_ok());
                let PushInfo { kind, data: _ } = result.unwrap();
                assert!(kind == PushKind::Message || kind == PushKind::PMessage);
                if kind == PushKind::Message {
                    msg_cnt += 1;
                } else {
                    pmsg_cnt += 1;
                }
            }
            assert_eq!(msg_cnt, 1);
            assert_eq!(pmsg_cnt, 2);

            if use_sharded {
                let result = cmd("SPUBLISH")
                    .arg("test_channel_?")
                    .arg("test_message")
                    .query_async(&mut connection)
                    .await;
                assert_eq!(result, Ok(Value::Int(1)));

                sleep(Duration::from_secs(1).into()).await;
                let result = rx.try_recv();
                assert!(result.is_ok());
                let PushInfo { kind, data } = result.unwrap();
                assert_eq!(
                    (kind, data),
                    (
                        PushKind::SMessage,
                        vec![
                            Value::BulkString("test_channel_?".into()),
                            Value::BulkString("test_message".into()),
                        ]
                    )
                );
            }

            Ok(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_config_only_subscriptions_still_work() {
        let client_subscriptions = PubSubSubscriptionInfo::from([(
            PubSubSubscriptionKind::Exact,
            HashSet::from([PubSubChannelOrPattern::from("config_test".as_bytes())]),
        )]);

        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| {
                builder
                    .use_protocol(ProtocolVersion::RESP3)
                    .pubsub_subscriptions(client_subscriptions.clone())
            },
            false,
        );

        block_on_all(async move {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
            let mut _connection = cluster.async_connection(Some(tx)).await;
            let mut publish_connection = cluster.async_connection(None).await;

            // Wait for subscriptions
            sleep(Duration::from_millis(300).into()).await;

            // Validate subscriptions happened
            validate_subscriptions(&client_subscriptions, &mut rx, false);

            // Publish and verify message received
            let result = retry_publish_until_expected_subscribers(
                PublishCommand::Publish,
                &mut publish_connection,
                "config_test",
                "test_message",
                2,
                10,
            )
            .await;
            assert_eq!(result, Ok(Value::Int(2)));

            sleep(Duration::from_millis(200).into()).await;
            let push = rx.try_recv().unwrap();
            assert_eq!(push.kind, PushKind::Message);

            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_mixed_config_and_dynamic_subscriptions() {
        let config_channel = PubSubChannelOrPattern::from("config_channel".as_bytes());
        let client_subscriptions = PubSubSubscriptionInfo::from([(
            PubSubSubscriptionKind::Exact,
            HashSet::from([config_channel.clone()]),
        )]);

        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| {
                builder
                    .use_protocol(ProtocolVersion::RESP3)
                    .pubsub_subscriptions(client_subscriptions.clone())
            },
            false,
        );

        block_on_all(async move {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
            let mut connection = cluster.async_connection(Some(tx)).await;

            // Wait for config subscriptions
            sleep(Duration::from_millis(300).into()).await;
            validate_subscriptions(&client_subscriptions, &mut rx, false);

            // Add dynamic subscription
            let dynamic_channel = PubSubChannelOrPattern::from("dynamic_channel".as_bytes());
            let _ = connection.subscribe(vec![dynamic_channel.clone()]).await;

            sleep(Duration::from_millis(300).into()).await;

            // Verify both are active
            let (desired, current) = connection.get_subscriptions().await.unwrap();
            let desired_channels = desired.get("channels").unwrap();
            let current_channels = current.get("channels").unwrap();

            assert!(desired_channels.contains(&config_channel));
            assert!(desired_channels.contains(&dynamic_channel));
            assert!(current_channels.contains(&config_channel));
            assert!(current_channels.contains(&dynamic_channel));

            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_receive_messages_after_dynamic_subscribe() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
            let mut connection = cluster.async_connection(Some(tx)).await;
            let mut publish_connection = cluster.async_connection(None).await;

            let channel = PubSubChannelOrPattern::from("message_test".as_bytes());

            // Dynamically subscribe
            let _ = connection.subscribe(vec![channel.clone()]).await;
            sleep(Duration::from_millis(300).into()).await;

            // Publish message
            let result = retry_publish_until_expected_subscribers(
                PublishCommand::Publish,
                &mut publish_connection,
                "message_test",
                "hello",
                1,
                10,
            )
            .await;
            assert_eq!(result, Ok(Value::Int(1)));

            sleep(Duration::from_millis(200).into()).await;

            // Verify message received
            loop {
                let push = rx.try_recv().unwrap();
                if push.kind == PushKind::Message {
                    assert_eq!(
                        push.data,
                        vec![
                            Value::BulkString("message_test".into()),
                            Value::BulkString("hello".into()),
                        ]
                    );
                    break;
                }
            }

            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_no_messages_after_unsubscribe() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
            let mut connection = cluster.async_connection(Some(tx)).await;
            let mut publish_connection = cluster.async_connection(None).await;

            let channel = PubSubChannelOrPattern::from("unsub_test".as_bytes());

            // Subscribe
            let _ = connection.subscribe(vec![channel.clone()]).await;
            sleep(Duration::from_millis(300).into()).await;

            // Drain subscription notification
            while let Ok(push) = rx.try_recv() {
                if push.kind == PushKind::Subscribe {
                    break;
                }
            }

            // Publish and verify message received
            let _ = retry_publish_until_expected_subscribers(
                PublishCommand::Publish,
                &mut publish_connection,
                "unsub_test",
                "msg1",
                1,
                10,
            )
            .await;

            sleep(Duration::from_millis(200).into()).await;
            let push = rx.try_recv().unwrap();
            assert_eq!(push.kind, PushKind::Message);

            // Unsubscribe
            let _ = connection.unsubscribe(Some(vec![channel])).await;
            sleep(Duration::from_millis(300).into()).await;

            // Publish again - should not receive message
            let result = cmd("PUBLISH")
                .arg("unsub_test")
                .arg("msg2")
                .query_async::<_, Value>(&mut publish_connection)
                .await
                .unwrap();

            // Should return 0 subscribers
            assert_eq!(result, Value::Int(0));

            sleep(Duration::from_millis(200).into()).await;

            // Should not receive message
            match rx.try_recv() {
                Ok(push) => {
                    // Only subscription-related pushes are acceptable
                    assert!(
                        push.kind == PushKind::Unsubscribe || push.kind == PushKind::Disconnection,
                        "Should not receive message after unsubscribe, got: {:?}",
                        push.kind
                    );
                }
                Err(mpsc::error::TryRecvError::Empty) => {
                    // This is the expected outcome
                }
                Err(e) => panic!("Unexpected error: {:?}", e),
            }

            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_multiple_channels_subscription() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            
            let channels = vec![
                PubSubChannelOrPattern::from("ch1".as_bytes()),
                PubSubChannelOrPattern::from("ch2".as_bytes()),
                PubSubChannelOrPattern::from("ch3".as_bytes()),
            ];
            
            // Subscribe to multiple channels in one call
            let result = connection.subscribe(channels.clone()).await;
            assert!(result.is_ok());
            
            sleep(Duration::from_millis(300).into()).await;
            
            let (desired, current) = connection.get_subscriptions().await.unwrap();
            let desired_channels = desired.get("channels").unwrap();
            let current_channels = current.get("channels").unwrap();
            
            // Verify all channels are in both desired and current
            for channel in &channels {
                assert!(desired_channels.contains(channel));
                assert!(current_channels.contains(channel));
            }
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_punsubscribe_all_patterns() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            
            let patterns = vec![
                PubSubChannelOrPattern::from("pattern1_*".as_bytes()),
                PubSubChannelOrPattern::from("pattern2_*".as_bytes()),
            ];
            
            // Subscribe to patterns
            let _ = connection.psubscribe(patterns).await;
            sleep(Duration::from_millis(300).into()).await;
            
            // Unsubscribe from all patterns (None means all)
            let result = connection.punsubscribe(None).await;
            assert!(result.is_ok());
            
            sleep(Duration::from_millis(300).into()).await;
            
            let (desired, current) = connection.get_subscriptions().await.unwrap();
            
            let empty_set = HashSet::new();  // Fix: create binding for empty set
            let desired_patterns = desired.get("patterns").unwrap_or(&empty_set);
            let current_patterns = current.get("patterns").unwrap_or(&empty_set);
            
            assert!(desired_patterns.is_empty());
            assert!(current_patterns.is_empty());
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_sunsubscribe_all_sharded_channels() {
        block_on_all(async move {
            if engine_version_less_than("7.0").await {
                return Ok::<_, RedisError>(());
            }

            let cluster = TestClusterContext::new_with_cluster_client_builder(
                3,
                0,
                |builder| builder.use_protocol(ProtocolVersion::RESP3),
                false,
            );

            let mut connection = cluster.async_connection(None).await;
            
            let channels = vec![
                PubSubChannelOrPattern::from("shard1".as_bytes()),
                PubSubChannelOrPattern::from("shard2".as_bytes()),
            ];
            
            // Subscribe to sharded channels
            let _ = connection.ssubscribe(channels).await;
            sleep(Duration::from_millis(300).into()).await;
            
            // Unsubscribe from all (None means all)
            let result = connection.sunsubscribe(None).await;
            assert!(result.is_ok());
            
            sleep(Duration::from_millis(300).into()).await;
            
            let (desired, current) = connection.get_subscriptions().await.unwrap();
            
            let empty_set = HashSet::new();  // Fix: create binding for empty set
            let desired_sharded = desired.get("sharded_channels").unwrap_or(&empty_set);
            let current_sharded = current.get("sharded_channels").unwrap_or(&empty_set);
            
            assert!(desired_sharded.is_empty());
            assert!(current_sharded.is_empty());
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_subscription_state_consistency_after_rapid_changes() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            
            let channels: Vec<_> = (0..10)
                .map(|i| PubSubChannelOrPattern::from(format!("rapid_ch{}", i).as_bytes()))
                .collect();
            
            // Rapidly add and remove subscriptions
            for channel in &channels {
                let _ = connection.subscribe(vec![channel.clone()]).await;
            }
            
            // Remove some
            for i in 0..5 {
                let _ = connection.unsubscribe(Some(vec![channels[i].clone()])).await;
            }
            
            // Add them back
            for i in 0..5 {
                let _ = connection.subscribe(vec![channels[i].clone()]).await;
            }
            
            // Wait for reconciliation
            sleep(Duration::from_millis(500).into()).await;
            
            // Verify final state - all 10 should be subscribed
            let (desired, current) = connection.get_subscriptions().await.unwrap();
            let desired_channels = desired.get("channels").unwrap();
            let current_channels = current.get("channels").unwrap();
            
            assert_eq!(desired_channels.len(), 10);
            assert_eq!(current_channels.len(), 10);
            
            for channel in &channels {
                assert!(desired_channels.contains(channel));
                assert!(current_channels.contains(channel));
            }
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_subscription_patterns_work_correctly() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
            let mut _connection = cluster.async_connection(Some(tx)).await;  // Fix: prefix with underscore, remove mut
            let mut publish_connection = cluster.async_connection(None).await;

            let pattern = PubSubChannelOrPattern::from("test_*".as_bytes());

            // Subscribe to pattern
            let _ = _connection.psubscribe(vec![pattern]).await;
            sleep(Duration::from_millis(300).into()).await;

            // Drain subscription notification
            while let Ok(push) = rx.try_recv() {
                if push.kind == PushKind::PSubscribe {
                    break;
                }
            }

            // Publish to matching channel
            let _ = cmd("PUBLISH")
                .arg("test_channel_123")
                .arg("pattern_message")
                .query_async::<_, Value>(&mut publish_connection)
                .await;

            sleep(Duration::from_millis(200).into()).await;

            // Verify pattern message received
            let push = rx.try_recv().unwrap();
            assert_eq!(push.kind, PushKind::PMessage);

            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_lazy_rapid_operations_and_idempotency() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            
            // Part 1: Test rapid operations with many channels
            let channels: Vec<_> = (0..20)
                .map(|i| PubSubChannelOrPattern::from(format!("rapid_{}", i).as_bytes()))
                .collect();
            
            let start = std::time::Instant::now();
            
            // Rapidly subscribe to 20 channels using lazy mode
            for channel in &channels {
                let _ = connection.subscribe_lazy(vec![channel.clone()]).await;
            }
            
            let elapsed = start.elapsed();
            
            // All 20 operations should complete very quickly (no waiting for server)
            assert!(elapsed < Duration::from_millis(100),
                "20 lazy subscribes took {:?}, should be < 100ms", elapsed);
            
            // Part 2: Test idempotency - subscribe to same channel multiple times
            let duplicate_channel = PubSubChannelOrPattern::from("duplicate_test".as_bytes());
            
            let _ = connection.subscribe_lazy(vec![duplicate_channel.clone()]).await;
            let _ = connection.subscribe_lazy(vec![duplicate_channel.clone()]).await;
            let _ = connection.subscribe_lazy(vec![duplicate_channel.clone()]).await;
            
            // Desired state should have all unique channels (20 + 1)
            let (desired, _) = connection.get_subscriptions().await.unwrap();
            let desired_channels = desired.get("channels").unwrap();
            assert_eq!(desired_channels.len(), 21, "Should have 21 unique channels");
            
            for channel in &channels {
                assert!(desired_channels.contains(channel));
            }
            assert!(desired_channels.contains(&duplicate_channel));
            
            // Wait for reconciliation to complete
            sleep(Duration::from_millis(600).into()).await;
            
            // Current state should eventually match and have exactly 21 unique channels
            let (_, current) = connection.get_subscriptions().await.unwrap();
            let current_channels = current.get("channels").unwrap();
            assert_eq!(current_channels.len(), 21, 
                "Should have 21 unique channels in current (no duplicates)");
            
            for channel in &channels {
                assert!(current_channels.contains(channel), 
                    "Channel {} should be in current", String::from_utf8_lossy(channel));
            }
            assert!(current_channels.contains(&duplicate_channel),
                "Duplicate channel should be in current exactly once");
            
            // Part 3: Test rapid unsubscribe with idempotency
            // Unsubscribe from first 10 channels multiple times
            for i in 0..10 {
                let _ = connection.unsubscribe_lazy(Some(vec![channels[i].clone()])).await;
                // Duplicate unsubscribe - should be idempotent
                let _ = connection.unsubscribe_lazy(Some(vec![channels[i].clone()])).await;
            }
            
            // Desired should have 11 channels (10 remaining from rapid + 1 duplicate)
            let (desired, _) = connection.get_subscriptions().await.unwrap();
            let desired_channels = desired.get("channels").unwrap();
            assert_eq!(desired_channels.len(), 11);
            
            // Wait for reconciliation
            sleep(Duration::from_millis(600).into()).await;
            
            // Current should match desired (11 channels)
            let (_, current) = connection.get_subscriptions().await.unwrap();
            let current_channels = current.get("channels").unwrap();
            assert_eq!(current_channels.len(), 11);
            
            // Verify correct channels remain
            for i in 10..20 {
                assert!(current_channels.contains(&channels[i]));
            }
            assert!(current_channels.contains(&duplicate_channel));
            
            // Verify unsubscribed channels are gone
            for i in 0..10 {
                assert!(!current_channels.contains(&channels[i]));
            }
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_lazy_vs_blocking_timing_difference() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            
            let lazy_channel = PubSubChannelOrPattern::from("lazy_ch".as_bytes());
            let blocking_channel = PubSubChannelOrPattern::from("blocking_ch".as_bytes());
            
            // Test lazy mode timing
            let lazy_start = std::time::Instant::now();
            let _ = connection.subscribe_lazy(vec![lazy_channel.clone()]).await;
            let lazy_elapsed = lazy_start.elapsed();
            
            // Test blocking mode timing
            let blocking_start = std::time::Instant::now();
            let _ = connection.subscribe(vec![blocking_channel.clone()]).await;
            let blocking_elapsed = blocking_start.elapsed();
            
            eprintln!("Lazy took: {:?}", lazy_elapsed);
            eprintln!("Blocking took: {:?}", blocking_elapsed);
            
            // Lazy should be much faster (< 50ms)
            assert!(lazy_elapsed < Duration::from_millis(50));
            
            // Blocking should wait for confirmation (> 50ms)
            assert!(blocking_elapsed > Duration::from_millis(50));
            
            // Both should eventually be subscribed
            sleep(Duration::from_millis(200).into()).await;
            let (_, current) = connection.get_subscriptions().await.unwrap();
            let current_channels = current.get("channels").unwrap();
            
            assert!(current_channels.contains(&lazy_channel));
            assert!(current_channels.contains(&blocking_channel));
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_lazy_unsubscribe_all_channels() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            let mut publish_connection = cluster.async_connection(None).await;
            
            let channels = vec![
                PubSubChannelOrPattern::from("ch1".as_bytes()),
                PubSubChannelOrPattern::from("ch2".as_bytes()),
                PubSubChannelOrPattern::from("ch3".as_bytes()),
            ];
            
            // Subscribe to all
            for channel in &channels {
                let _ = connection.subscribe(vec![channel.clone()]).await;
            }
            
            sleep(Duration::from_millis(300).into()).await;
            
            // Verify subscribed by publishing
            let result = retry_publish_until_expected_subscribers(
                PublishCommand::Publish,
                &mut publish_connection,
                "ch1",
                "test",
                1,
                10,
            )
            .await;
            assert_eq!(result, Ok(Value::Int(1)));
            
            // Unsubscribe from all using lazy (None means all)
            let start = std::time::Instant::now();
            let result = connection.unsubscribe_lazy(None).await;
            let elapsed = start.elapsed();
            
            assert!(result.is_ok());
            assert!(elapsed < Duration::from_millis(50));
            
            // Desired state cleared immediately
            let (desired, _) = connection.get_subscriptions().await.unwrap();
            let empty_set = HashSet::new();
            assert!(desired.get("channels").unwrap_or(&empty_set).is_empty());
            
            // Wait for reconciliation
            sleep(Duration::from_millis(300).into()).await;
            
            // Current state eventually cleared
            let (_, current) = connection.get_subscriptions().await.unwrap();
            assert!(current.get("channels").unwrap_or(&empty_set).is_empty());
            
            // Verify actually unsubscribed by publishing
            let result = retry_publish_until_expected_subscribers(
                PublishCommand::Publish,
                &mut publish_connection,
                "ch1",
                "should_not_receive",
                0, 
                10,
            )
            .await;
            assert_eq!(result, Ok(Value::Int(0)));;
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_lazy_punsubscribe_all_patterns() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
            let mut connection = cluster.async_connection(Some(tx)).await;
            let mut publish_connection = cluster.async_connection(None).await;
            
            let patterns = vec![
                PubSubChannelOrPattern::from("pattern1_*".as_bytes()),
                PubSubChannelOrPattern::from("pattern2_*".as_bytes()),
            ];
            
            // Subscribe to patterns
            for pattern in &patterns {
                let _ = connection.psubscribe(vec![pattern.clone()]).await;
            }
            
            sleep(Duration::from_millis(300).into()).await;
            
            // Drain subscription notifications
            while let Ok(push) = rx.try_recv() {
                if push.kind == PushKind::PSubscribe {
                    // Keep draining
                }
            }
            
            // Verify subscribed by publishing to matching channel and receiving message
            let _ = cmd("PUBLISH")
                .arg("pattern1_test")
                .arg("before_unsub")
                .query_async::<_, Value>(&mut publish_connection)
                .await
                .unwrap();
            
            sleep(Duration::from_millis(200).into()).await;
            
            // Should receive pattern message
            let push = rx.try_recv().unwrap();
            assert_eq!(push.kind, PushKind::PMessage);
            assert_eq!(
                push.data,
                vec![
                    Value::BulkString("pattern1_*".into()),
                    Value::BulkString("pattern1_test".into()),
                    Value::BulkString("before_unsub".into()),
                ]
            );
            
            // Unsubscribe from all using lazy
            let start = std::time::Instant::now();
            let result = connection.punsubscribe_lazy(None).await;
            let elapsed = start.elapsed();
            
            assert!(result.is_ok());
            assert!(elapsed < Duration::from_millis(50));
            
            // Desired state cleared immediately
            let (desired, _) = connection.get_subscriptions().await.unwrap();
            let empty_set = HashSet::new();
            assert!(desired.get("patterns").unwrap_or(&empty_set).is_empty());
            
            // Wait for reconciliation
            sleep(Duration::from_millis(300).into()).await;
            
            // Current state eventually cleared
            let (_, current) = connection.get_subscriptions().await.unwrap();
            assert!(current.get("patterns").unwrap_or(&empty_set).is_empty());
            
            // Verify actually unsubscribed by publishing and NOT receiving message
            let _ = cmd("PUBLISH")
                .arg("pattern1_test")
                .arg("should_not_receive")
                .query_async::<_, Value>(&mut publish_connection)
                .await
                .unwrap();
            
            sleep(Duration::from_millis(200).into()).await;
            
            // Should NOT receive message (or only unsubscribe notification)
            match rx.try_recv() {
                Err(mpsc::error::TryRecvError::Empty) => {
                    // Expected - no messages
                }
                Ok(push) => {
                    // Only unsubscribe-related pushes are acceptable
                    assert!(
                        push.kind == PushKind::PUnsubscribe || push.kind == PushKind::Disconnection,
                        "Should not receive PMessage after punsubscribe, got: {:?}",
                        push.kind
                    );
                }
                Err(e) => panic!("Unexpected error: {:?}", e),
            }
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_lazy_sunsubscribe_all_sharded() {
        block_on_all(async move {
            if engine_version_less_than("7.0").await {
                return Ok::<_, RedisError>(());
            }

            let cluster = TestClusterContext::new_with_cluster_client_builder(
                3,
                0,
                |builder| builder.use_protocol(ProtocolVersion::RESP3),
                false,
            );

            let mut connection = cluster.async_connection(None).await;
            let mut publish_connection = cluster.async_connection(None).await;
            
            let channels = vec![
                PubSubChannelOrPattern::from("shard1".as_bytes()),
                PubSubChannelOrPattern::from("shard2".as_bytes()),
            ];
            
            // Subscribe
            for channel in &channels {
                let _ = connection.ssubscribe(vec![channel.clone()]).await;
            }
            
            sleep(Duration::from_millis(300).into()).await;
            
            // Verify subscribed by publishing
            let result = retry_publish_until_expected_subscribers(
                PublishCommand::SPublish,
                &mut publish_connection,
                "shard1",
                "test",
                1,
                10,
            )
            .await;
            assert_eq!(result, Ok(Value::Int(1)));
            
            // Unsubscribe from all using lazy
            let start = std::time::Instant::now();
            let result = connection.sunsubscribe_lazy(None).await;
            let elapsed = start.elapsed();
            
            assert!(result.is_ok());
            assert!(elapsed < Duration::from_millis(50));
            
            // Desired state cleared immediately
            let (desired, _) = connection.get_subscriptions().await.unwrap();
            let empty_set = HashSet::new();
            assert!(desired.get("sharded_channels").unwrap_or(&empty_set).is_empty());
            
            // Wait for reconciliation AND fenced commands to complete
            sleep(Duration::from_millis(500).into()).await;
            
            // Current state eventually cleared
            let (_, current) = connection.get_subscriptions().await.unwrap();
            assert!(current.get("sharded_channels").unwrap_or(&empty_set).is_empty());
            
            // Verify actually unsubscribed
            let result = cmd("SPUBLISH")
                .arg("shard1")
                .arg("should_not_receive")
                .query_async::<_, Value>(&mut publish_connection)
                .await
                .unwrap();
            
            assert_eq!(result, Value::Int(0));
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_lazy_mixed_subscribe_types() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            
            let channel = PubSubChannelOrPattern::from("exact_lazy".as_bytes());
            let pattern = PubSubChannelOrPattern::from("pattern_lazy_*".as_bytes());
            
            let start = std::time::Instant::now();
            
            // Subscribe to both using lazy
            let _ = connection.subscribe_lazy(vec![channel.clone()]).await;
            let _ = connection.psubscribe_lazy(vec![pattern.clone()]).await;
            
            let elapsed = start.elapsed();
            
            // Both should complete very quickly
            assert!(elapsed < Duration::from_millis(50));
            
            // Both in desired immediately
            let (desired, _) = connection.get_subscriptions().await.unwrap();
            assert!(desired.get("channels").unwrap().contains(&channel));
            assert!(desired.get("patterns").unwrap().contains(&pattern));
            
            // Wait for reconciliation
            sleep(Duration::from_millis(300).into()).await;
            
            // Both in current eventually
            let (_, current) = connection.get_subscriptions().await.unwrap();
            assert!(current.get("channels").unwrap().contains(&channel));
            assert!(current.get("patterns").unwrap().contains(&pattern));
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_lazy_rapid_subscribe_unsubscribe() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            
            let channel = PubSubChannelOrPattern::from("rapid_lazy".as_bytes());
            
            let start = std::time::Instant::now();
            
            // Rapidly subscribe and unsubscribe 10 times
            for _ in 0..10 {
                let _ = connection.subscribe_lazy(vec![channel.clone()]).await;
                let _ = connection.unsubscribe_lazy(Some(vec![channel.clone()])).await;
            }
            
            // Final subscribe
            let _ = connection.subscribe_lazy(vec![channel.clone()]).await;
            
            let elapsed = start.elapsed();
            
            // All 21 operations should be very fast
            assert!(elapsed < Duration::from_millis(200),
                "21 lazy operations took {:?}, should be < 200ms", elapsed);
            
            // Wait for reconciliation to settle
            sleep(Duration::from_millis(500).into()).await;
            
            // Final state should be subscribed
            let (desired, current) = connection.get_subscriptions().await.unwrap();
            assert!(desired.get("channels").unwrap().contains(&channel));
            assert!(current.get("channels").unwrap().contains(&channel));
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_lazy_operations_work_with_messages() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
            let mut connection = cluster.async_connection(Some(tx)).await;
            let mut publish_connection = cluster.async_connection(None).await;

            let channel = PubSubChannelOrPattern::from("lazy_msg_test".as_bytes());

            // Lazy subscribe
            let _ = connection.subscribe_lazy(vec![channel.clone()]).await;
            
            // Wait for reconciliation
            sleep(Duration::from_millis(300).into()).await;

            // Drain subscription notification
            while let Ok(push) = rx.try_recv() {
                if push.kind == PushKind::Subscribe {
                    break;
                }
            }

            // Publish and verify message received
            let _ = retry_publish_until_expected_subscribers(
                PublishCommand::Publish,
                &mut publish_connection,
                "lazy_msg_test",
                "hello",
                1,
                10,
            )
            .await;

            sleep(Duration::from_millis(200).into()).await;

            // Should receive message
            let push = rx.try_recv().unwrap();
            assert_eq!(push.kind, PushKind::Message);
            assert_eq!(
                push.data,
                vec![
                    Value::BulkString("lazy_msg_test".into()),
                    Value::BulkString("hello".into()),
                ]
            );

            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_lazy_no_messages_after_unsubscribe() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<PushInfo>();
            let mut connection = cluster.async_connection(Some(tx)).await;
            let mut publish_connection = cluster.async_connection(None).await;

            let channel = PubSubChannelOrPattern::from("lazy_unsub_msg".as_bytes());

            // Lazy subscribe
            let _ = connection.subscribe_lazy(vec![channel.clone()]).await;
            sleep(Duration::from_millis(300).into()).await;

            // Drain subscription notification
            while let Ok(_) = rx.try_recv() {}

            // Lazy unsubscribe
            let _ = connection.unsubscribe_lazy(Some(vec![channel])).await;
            sleep(Duration::from_millis(300).into()).await;

            // Publish - should not receive message
            let result = cmd("PUBLISH")
                .arg("lazy_unsub_msg")
                .arg("should_not_receive")
                .query_async::<_, Value>(&mut publish_connection)
                .await
                .unwrap();

            // Should return 0 subscribers
            assert_eq!(result, Value::Int(0));

            sleep(Duration::from_millis(200).into()).await;

            // Should not receive message
            match rx.try_recv() {
                Err(mpsc::error::TryRecvError::Empty) => {
                    // Expected - no messages
                }
                Ok(push) => {
                    // Only subscription-related pushes are acceptable
                    assert!(
                        push.kind != PushKind::Message,
                        "Should not receive message after lazy unsubscribe"
                    );
                }
                Err(e) => panic!("Unexpected error: {:?}", e),
            }

            Ok::<_, RedisError>(())
        })
        .unwrap();
    }

    #[test]
    #[serial_test::serial]
    fn test_async_cluster_lazy_multiple_channels_single_call() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(
            3,
            0,
            |builder| builder.use_protocol(ProtocolVersion::RESP3),
            false,
        );

        block_on_all(async move {
            let mut connection = cluster.async_connection(None).await;
            
            let channels = vec![
                PubSubChannelOrPattern::from("multi_lazy_1".as_bytes()),
                PubSubChannelOrPattern::from("multi_lazy_2".as_bytes()),
                PubSubChannelOrPattern::from("multi_lazy_3".as_bytes()),
            ];
            
            let start = std::time::Instant::now();
            
            // Subscribe to multiple channels in one lazy call
            let result = connection.subscribe_lazy(channels.clone()).await;
            
            let elapsed = start.elapsed();
            
            assert!(result.is_ok());
            assert!(elapsed < Duration::from_millis(50));
            
            // All in desired immediately
            let (desired, _) = connection.get_subscriptions().await.unwrap();
            let desired_channels = desired.get("channels").unwrap();
            
            for channel in &channels {
                assert!(desired_channels.contains(channel));
            }
            
            // Wait for reconciliation
            sleep(Duration::from_millis(400).into()).await;
            
            // All in current eventually
            let (_, current) = connection.get_subscriptions().await.unwrap();
            let current_channels = current.get("channels").unwrap();
            
            for channel in &channels {
                assert!(current_channels.contains(channel));
            }
            
            Ok::<_, RedisError>(())
        })
        .unwrap();
    }
}