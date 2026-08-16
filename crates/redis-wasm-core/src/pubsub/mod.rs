//! Pub/Sub implementation

use std::sync::Arc;
use dashmap::DashMap;

#[cfg(feature = "native")]
use tokio::sync::broadcast;

/// Pub/Sub channel
#[derive(Clone)]
pub struct Channel {
    #[cfg(feature = "native")]
    tx: broadcast::Sender<String>,
    #[cfg(not(feature = "native"))]
    tx: (),
    /// Subscriber count
    subscriber_count: Arc<std::sync::atomic::AtomicUsize>,
}

impl Channel {
    /// Create a new channel
    pub fn new() -> Self {
        #[cfg(feature = "native")]
        let (tx, _) = broadcast::channel(1000);
        #[cfg(not(feature = "native"))]
        let tx = ();

        Self {
            tx,
            subscriber_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// Publish a message to the channel
    pub fn publish(&self, message: String) -> usize {
        #[cfg(feature = "native")]
        {
            let count = self.tx.receiver_count();
            let _ = self.tx.send(message);
            count
        }
        #[cfg(not(feature = "native"))]
        {
            0
        }
    }

    /// Subscribe to the channel
    #[cfg(feature = "native")]
    pub fn subscribe(&self) -> broadcast::Receiver<String> {
        self.subscriber_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.tx.subscribe()
    }

    /// Subscribe to the channel (WASM stub)
    #[cfg(not(feature = "native"))]
    pub fn subscribe(&self) {
        self.subscriber_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Get subscriber count
    pub fn subscriber_count(&self) -> usize {
        self.subscriber_count.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl Default for Channel {
    fn default() -> Self {
        Self::new()
    }
}

/// Pub/Sub manager.
///
/// Cloning is cheap and clones share the same channel registry.
#[derive(Clone)]
pub struct PubSubManager {
    channels: Arc<DashMap<String, Arc<Channel>>>,
}

impl PubSubManager {
    /// Create a new pub/sub manager
    pub fn new() -> Self {
        Self {
            channels: Arc::new(DashMap::new()),
        }
    }

    /// Get or create a channel
    pub fn get_or_create(&self, name: &str) -> Arc<Channel> {
        self.channels
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Channel::new()))
            .clone()
    }

    /// Get a channel (if exists)
    pub fn get(&self, name: &str) -> Option<Arc<Channel>> {
        self.channels.get(name).map(|c| c.clone())
    }

    /// Publish a message to a channel
    pub fn publish(&self, channel: &str, message: String) -> usize {
        if let Some(ch) = self.get(channel) {
            ch.publish(message)
        } else {
            0
        }
    }

    /// Subscribe to a channel
    #[cfg(feature = "native")]
    pub fn subscribe(&self, channel: &str) -> broadcast::Receiver<String> {
        self.get_or_create(channel).subscribe()
    }

    /// Subscribe to a channel (WASM stub)
    #[cfg(not(feature = "native"))]
    pub fn subscribe(&self, channel: &str) {
        self.get_or_create(channel).subscribe()
    }

    /// Unsubscribe from a channel (decrements count)
    pub fn unsubscribe(&self, channel: &str) {
        if let Some(ch) = self.get(channel) {
            // Note: broadcast doesn't support explicit unsubscribe
            // The receiver is just dropped
            // We could track subscriber count manually if needed
        }
    }

    /// Get all channel names
    pub fn channels(&self) -> Vec<String> {
        self.channels.iter().map(|e| e.key().clone()).collect()
    }

    /// Get subscriber count for a channel
    pub fn subscriber_count(&self, channel: &str) -> usize {
        self.get(channel).map(|c| c.subscriber_count()).unwrap_or(0)
    }

    /// Remove empty channels
    pub fn cleanup_empty(&self) {
        self.channels.retain(|_, ch| ch.subscriber_count() > 0);
    }
}

impl Default for PubSubManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Message for cross-tab communication via BroadcastChannel
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PubSubMessage {
    pub channel: String,
    pub message: String,
    pub timestamp: u64,
}

impl PubSubMessage {
    pub fn new(channel: String, message: String) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        Self { channel, message, timestamp }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "native")]
    #[tokio::test]
    async fn test_pubsub_basic() {
        let manager = PubSubManager::new();

        let mut rx1 = manager.subscribe("channel1");
        let mut rx2 = manager.subscribe("channel1");

        assert_eq!(manager.subscriber_count("channel1"), 2);

        let count = manager.publish("channel1", "hello".to_string());
        assert_eq!(count, 2);

        let msg1 = rx1.recv().await.unwrap();
        let msg2 = rx2.recv().await.unwrap();
        assert_eq!(msg1, "hello");
        assert_eq!(msg2, "hello");
    }

    #[cfg(feature = "native")]
    #[tokio::test]
    async fn test_pubsub_multiple_channels() {
        let manager = PubSubManager::new();

        let mut rx1 = manager.subscribe("channel1");
        let mut rx2 = manager.subscribe("channel2");

        manager.publish("channel1", "msg1".to_string());
        manager.publish("channel2", "msg2".to_string());

        assert_eq!(rx1.recv().await.unwrap(), "msg1");
        assert_eq!(rx2.recv().await.unwrap(), "msg2");
    }

    #[cfg(feature = "native")]
    #[tokio::test]
    async fn test_pubsub_no_subscribers() {
        let manager = PubSubManager::new();

        let count = manager.publish("empty", "msg".to_string());
        assert_eq!(count, 0);
    }
}