//! WASM Pub/Sub: local subscribers within this JS context, plus cross-tab
//! delivery via the BroadcastChannel API.
//!
//! `BroadcastChannel` never delivers a message back to the context that
//! posted it, so publish sends to local subscribers directly and lets the
//! browser fan out to other tabs/workers.

use futures::channel::mpsc;
use futures::StreamExt;
use redis_wasm_core::pubsub::PubSubMessage;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::{BroadcastChannel, MessageEvent};

type Subscribers = Rc<RefCell<Vec<mpsc::UnboundedSender<String>>>>;

struct ChannelState {
    broadcast: BroadcastChannel,
    subscribers: Subscribers,
    /// Keeps the onmessage callback alive for the channel's lifetime.
    _onmessage: Closure<dyn FnMut(MessageEvent)>,
}

/// Pub/Sub hub for this JS context
#[wasm_bindgen]
pub struct WasmPubSub {
    channels: HashMap<String, ChannelState>,
}

impl Default for WasmPubSub {
    fn default() -> Self {
        Self::new()
    }
}

#[wasm_bindgen]
impl WasmPubSub {
    #[wasm_bindgen(constructor)]
    pub fn new() -> WasmPubSub {
        WasmPubSub {
            channels: HashMap::new(),
        }
    }

    /// Publish a message. Returns the number of local subscribers it was
    /// delivered to (cross-tab subscriber counts are unknowable).
    pub fn publish(&mut self, channel: &str, message: &str) -> Result<usize, JsValue> {
        let state = self.channel_state(channel)?;

        let envelope = PubSubMessage::new(channel.to_string(), message.to_string());
        let json =
            serde_json::to_string(&envelope).map_err(|e| JsValue::from_str(&e.to_string()))?;
        state
            .broadcast
            .post_message(&JsValue::from_str(&json))
            .map_err(|e| JsValue::from_str(&format!("Failed to post message: {:?}", e)))?;

        Ok(deliver_local(&state.subscribers, message))
    }

    /// Subscribe to a channel. The returned subscriber receives messages
    /// published in this context and in other tabs/workers.
    pub fn subscribe(&mut self, channel: &str) -> Result<WasmSubscriber, JsValue> {
        let state = self.channel_state(channel)?;
        let (tx, rx) = mpsc::unbounded();
        state.subscribers.borrow_mut().push(tx);
        Ok(WasmSubscriber { rx })
    }

    /// Number of local subscribers on a channel
    #[wasm_bindgen(js_name = "subscriberCount")]
    pub fn subscriber_count(&self, channel: &str) -> usize {
        self.channels
            .get(channel)
            .map(|state| {
                let mut subs = state.subscribers.borrow_mut();
                subs.retain(|tx| !tx.is_closed());
                subs.len()
            })
            .unwrap_or(0)
    }
}

impl WasmPubSub {
    fn channel_state(&mut self, name: &str) -> Result<&ChannelState, JsValue> {
        if !self.channels.contains_key(name) {
            let broadcast = BroadcastChannel::new(name).map_err(|e| {
                JsValue::from_str(&format!("Failed to create BroadcastChannel: {:?}", e))
            })?;

            let subscribers: Subscribers = Rc::new(RefCell::new(Vec::new()));
            let subs = subscribers.clone();
            let onmessage = Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
                if let Some(json) = event.data().as_string() {
                    if let Ok(envelope) = serde_json::from_str::<PubSubMessage>(&json) {
                        deliver_local(&subs, &envelope.message);
                    }
                }
            });
            broadcast.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));

            self.channels.insert(
                name.to_string(),
                ChannelState {
                    broadcast,
                    subscribers,
                    _onmessage: onmessage,
                },
            );
        }
        Ok(self.channels.get(name).unwrap())
    }
}

/// Send to every live local subscriber, pruning dropped ones
fn deliver_local(subscribers: &Subscribers, message: &str) -> usize {
    let mut subs = subscribers.borrow_mut();
    subs.retain(|tx| !tx.is_closed());
    let mut delivered = 0;
    for tx in subs.iter() {
        if tx.unbounded_send(message.to_string()).is_ok() {
            delivered += 1;
        }
    }
    delivered
}

/// Async message stream for one subscription. Drop it to unsubscribe.
#[wasm_bindgen]
pub struct WasmSubscriber {
    rx: mpsc::UnboundedReceiver<String>,
}

#[wasm_bindgen]
impl WasmSubscriber {
    /// Await the next message (resolves to undefined if the hub is dropped)
    pub async fn next(&mut self) -> Option<String> {
        self.rx.next().await
    }
}
