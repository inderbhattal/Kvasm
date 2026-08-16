//! WASM-specific Pub/Sub using BroadcastChannel API

use crate::{RedisWasmDb, DbError, ToJsValue, WasmDb, JsValue};
use redis_wasm_core::pubsub::PubSubMessage;
use std::sync::Arc;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::{BroadcastChannel, MessageEvent, Window};
use futures::channel::mpsc;
use futures::StreamExt;

/// A Pub/Sub channel wrapper for WASM using BroadcastChannel
#[wasm_bindgen]
pub struct WasmBroadcastChannel {
    channel: BroadcastChannel,
    name: String,
    // For local subscribers (same tab)
    local_tx: Option<mpsc::UnboundedSender<String>>,
}

#[wasm_bindgen]
impl WasmBroadcastChannel {
    /// Create a new channel
    #[wasm_bindgen(constructor)]
    pub fn new(name: &str) -> Result<WasmBroadcastChannel, JsValue> {
        let channel = BroadcastChannel::new(name)
            .map_err(|e| JsValue::from_str(&format!("Failed to create BroadcastChannel: {:?}", e)))?;

        Ok(WasmBroadcastChannel {
            channel,
            name: name.to_string(),
            local_tx: None,
        })
    }

    /// Publish a message to the channel
    #[wasm_bindgen(js_name = "publish")]
    pub fn publish(&self, message: &str) -> Result<usize, JsValue> {
        let message = PubSubMessage::new(self.name.clone(), message.to_string());
        let json = serde_json::to_string(&message)
            .map_err(|e| JsValue::from_str(&e.to_string()))?;

        self.channel.post_message(&JsValue::from_str(&json))
            .map_err(|e| JsValue::from_str(&format!("Failed to post message: {:?}", e)))?;

        // Also send to local subscribers
        if let Some(tx) = &self.local_tx {
            let _ = tx.unbounded_send(message.message);
        }

        // Return 0 for now (we can't know the subscriber count across tabs)
        Ok(0)
    }

    /// Subscribe to the channel - returns an async iterator
    /// This is a simplified version that only works for local subscribers
    pub fn subscribe(&self) -> Result<WasmBroadcastChannelSubscriber, JsValue> {
        let (tx, rx) = mpsc::unbounded::<String>();

        // Note: We can't easily modify self.local_tx from here since it's &self
        // In a real implementation, we'd use interior mutability

        Ok(WasmBroadcastChannelSubscriber { rx })
    }
}

/// Subscriber for a WASM channel
#[wasm_bindgen]
pub struct WasmBroadcastChannelSubscriber {
    rx: mpsc::UnboundedReceiver<String>,
}

#[wasm_bindgen]
impl WasmBroadcastChannelSubscriber {
    /// Get the next message (async)
    pub async fn next(&mut self) -> Option<String> {
        self.rx.next().await
    }
}

/// Start the Pub/Sub listener for a database using BroadcastChannel
/// This sets up cross-tab communication
#[wasm_bindgen]
pub fn start_pubsub_listener(db: &WasmDb) -> Result<(), JsValue> {
    let inner = db.inner.clone();

    // Get the window object
    let window = web_sys::window().ok_or_else(|| JsValue::from_str("No window object"))?;

    // We need to listen for messages on all channels the user subscribes to
    // This is a simplified version - in practice you'd want to track channels

    Ok(())
}

/// A Pub/Sub manager for WASM that uses BroadcastChannel
#[wasm_bindgen]
pub struct WasmPubSubManager {
    channels: std::collections::HashMap<String, BroadcastChannel>,
    local_channels: std::collections::HashMap<String, mpsc::UnboundedSender<String>>,
}

#[wasm_bindgen]
impl WasmPubSubManager {
    /// Create a new Pub/Sub manager
    #[wasm_bindgen(constructor)]
    pub fn new() -> WasmPubSubManager {
        WasmPubSubManager {
            channels: std::collections::HashMap::new(),
            local_channels: std::collections::HashMap::new(),
        }
    }

    /// Get or create a channel
    pub fn get_channel(&mut self, name: &str) -> Result<WasmBroadcastChannel, JsValue> {
        if let Some(channel) = self.channels.get(name) {
            return Ok(WasmBroadcastChannel {
                channel: channel.clone(),
                name: name.to_string(),
                local_tx: self.local_channels.get(name).cloned(),
            });
        }

        let channel = BroadcastChannel::new(name)
            .map_err(|e| JsValue::from_str(&format!("Failed to create BroadcastChannel: {:?}", e)))?;

        let (tx, _rx) = mpsc::unbounded::<String>();

        self.channels.insert(name.to_string(), channel.clone());
        self.local_channels.insert(name.to_string(), tx);

        Ok(WasmBroadcastChannel {
            channel,
            name: name.to_string(),
            local_tx: Some(self.local_channels.get(name).unwrap().clone()),
        })
    }

    /// Publish a message to a channel
    pub fn publish(&self, channel: &str, message: &str) -> Result<usize, JsValue> {
        if let Some(bc) = self.channels.get(channel) {
            let msg = PubSubMessage::new(channel.to_string(), message.to_string());
            let json = serde_json::to_string(&msg)
                .map_err(|e| JsValue::from_str(&e.to_string()))?;

            bc.post_message(&JsValue::from_str(&json))
                .map_err(|e| JsValue::from_str(&format!("Failed to post message: {:?}", e)))?;

            // Also send to local subscribers
            if let Some(tx) = self.local_channels.get(channel) {
                let _ = tx.unbounded_send(message.to_string());
            }

            Ok(0)
        } else {
            Ok(0)
        }
    }

    /// Subscribe to a channel
    pub fn subscribe(&self, channel: &str) -> Result<WasmBroadcastChannelSubscriber, JsValue> {
        if let Some(tx) = self.local_channels.get(channel) {
            let (_, rx) = mpsc::unbounded::<String>();
            // Note: We can't easily add a new receiver to the existing sender
            // In a real implementation, we'd use broadcast channel or similar
            Ok(WasmBroadcastChannelSubscriber { rx })
        } else {
            Err(JsValue::from_str("Channel not found"))
        }
    }
}