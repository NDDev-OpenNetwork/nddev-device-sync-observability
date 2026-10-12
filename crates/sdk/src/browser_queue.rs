//! Authenticated export spool. Records already passed the canonical redaction
//! boundary; IndexedDB stores exact event/OTLP batches and their receipt state.
//! Closing a realm cannot promise to persist events that have not committed.
use crate::{Error, ExportBatch, FeedHealth};
use serde::{Deserialize, Serialize};
use wasm_bindgen::{JsCast, prelude::*};
use wasm_bindgen_futures::JsFuture;

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Signal {
    Events,
    Traces,
}
#[derive(Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Health {
    pub events: FeedHealth,
    pub traces: FeedHealth,
    pub enabled: bool,
    pub last_success_at: u64,
    pub revision: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Metadata {
    id: String,
    signal: Signal,
    count: u32,
    expires_at_ms: u64,
    attempts: u8,
}
pub struct Batch {
    metadata: Metadata,
    bytes: Vec<u8>,
}
impl Batch {
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn count(&self) -> u32 {
        self.metadata.count
    }
    pub fn attempts(&self) -> u8 {
        self.metadata.attempts
    }
    pub fn signal(&self) -> Signal {
        self.metadata.signal
    }
    pub fn expired(&self) -> bool {
        chrono::Utc::now().timestamp_millis().max(0) as u64 >= self.metadata.expires_at_ms
    }
}
#[wasm_bindgen(module = "/src/browser_queue.js")]
extern "C" {
    #[wasm_bindgen(catch,js_name=queueOperation)]
    fn operation(
        scope: &str,
        command: &str,
        metadata: &str,
        bytes: &[u8],
    ) -> Result<js_sys::Promise, JsValue>;
    #[wasm_bindgen(catch,js_name=acquireSender)]
    fn acquire(scope: &str, id: &str) -> Result<js_sys::Promise, JsValue>;
    #[wasm_bindgen(js_name=releaseSender)]
    fn release(id: &str);
}
#[derive(Clone)]
pub struct Queue {
    scope: String,
}
impl Queue {
    pub fn new(scope: String) -> Result<Self, Error> {
        if scope.len() != 64
            || !scope
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(Error);
        }
        Ok(Self { scope })
    }
    async fn call(&self, command: &str, metadata: &str, bytes: &[u8]) -> Result<JsValue, Error> {
        JsFuture::from(operation(&self.scope, command, metadata, bytes).map_err(|_| Error)?)
            .await
            .map_err(|_| Error)
    }
    fn health_result(value: JsValue) -> Result<Health, Error> {
        let text = value.as_string().filter(|v| v.len() <= 4096).ok_or(Error)?;
        serde_json::from_str(&text).map_err(|_| Error)
    }
    pub async fn health(&self) -> Result<Health, Error> {
        Self::health_result(self.call("health", "", &[]).await?)
    }
    pub async fn record_losses(
        &self,
        events: FeedHealth,
        traces: FeedHealth,
    ) -> Result<Health, Error> {
        let encoded = serde_json::to_string(&serde_json::json!({
            "events": {"dropped":events.dropped,"failures":events.failures},
            "traces": {"dropped":traces.dropped,"failures":traces.failures}
        }))
        .map_err(|_| Error)?;
        Self::health_result(self.call("losses", &encoded, &[]).await?)
    }
    pub async fn set_enabled(&self, enabled: bool) -> Result<Health, Error> {
        Self::health_result(
            self.call(if enabled { "enable" } else { "disable" }, "", &[])
                .await?,
        )
    }
    /// The spawned writer retains ownership through actual IDB completion even
    /// when its caller is cancelled. On an unknown/failed write the RAM feed
    /// reports loss; committed batches remain available for exact-ID replay.
    pub async fn persist(&self, signal: Signal, batch: ExportBatch) -> Result<Health, Error> {
        let metadata = Metadata {
            id: uuid::Uuid::new_v4().to_string(),
            signal,
            count: batch.count(),
            expires_at_ms: batch.expires_at_ms()?,
            attempts: 0,
        };
        let encoded = serde_json::to_string(&metadata).map_err(|_| Error)?;
        let queue = self.clone();
        let (send, receive) = futures::channel::oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            let result = queue
                .call("append", &encoded, batch.bytes())
                .await
                .and_then(Self::health_result);
            if result.is_ok() {
                batch.persisted();
            } else {
                batch.failed();
            }
            let _ = send.send(result);
        });
        receive.await.map_err(|_| Error)?
    }
    /// Browser-owned Web Lock: closing a tab releases it, without a stale
    /// persistent claim. Transport retries may repeat already accepted spans.
    pub async fn sender(&self) -> Result<Option<Sender>, Error> {
        let sender = Sender {
            id: uuid::Uuid::new_v4().to_string(),
            scope: self.scope.clone(),
        };
        let acquired = JsFuture::from(acquire(&self.scope, &sender.id).map_err(|_| Error)?)
            .await
            .map_err(|_| Error)?
            .as_bool()
            .ok_or(Error)?;
        Ok(acquired.then_some(sender))
    }
}
pub struct Sender {
    id: String,
    scope: String,
}
impl Drop for Sender {
    fn drop(&mut self) {
        release(&self.id);
    }
}
impl Sender {
    pub async fn next(&self) -> Result<Option<Batch>, Error> {
        let value = Queue {
            scope: self.scope.clone(),
        }
        .call("next", &self.id, &[])
        .await?;
        if value.is_null() {
            return Ok(None);
        }
        let parts = value.dyn_into::<js_sys::Array>().map_err(|_| Error)?;
        if parts.length() != 2 {
            return Err(Error);
        }
        let metadata: Metadata = serde_json::from_str(
            &parts
                .get(0)
                .as_string()
                .filter(|v| v.len() <= 1024)
                .ok_or(Error)?,
        )
        .map_err(|_| Error)?;
        let bytes = parts
            .get(1)
            .dyn_into::<js_sys::Uint8Array>()
            .map_err(|_| Error)?
            .to_vec();
        if bytes.is_empty()
            || bytes.len() > 65536
            || metadata.count == 0
            || metadata.count > 32
            || metadata.attempts > 8
            || uuid::Uuid::parse_str(&metadata.id).is_err()
        {
            return Err(Error);
        }
        Ok(Some(Batch { metadata, bytes }))
    }
    pub async fn acknowledge(&self, batch: &Batch) -> Result<Health, Error> {
        self.finish("acknowledge", batch).await
    }
    pub async fn failed(&self, batch: &Batch) -> Result<Health, Error> {
        self.finish("failed", batch).await
    }
    pub async fn discard(&self, batch: &Batch) -> Result<Health, Error> {
        self.finish("discard", batch).await
    }
    async fn finish(&self, command: &str, batch: &Batch) -> Result<Health, Error> {
        let metadata =
            serde_json::to_string(&serde_json::json!({"sender":self.id,"batch":batch.metadata.id}))
                .map_err(|_| Error)?;
        Queue::health_result(
            Queue {
                scope: self.scope.clone(),
            }
            .call(command, &metadata, &[])
            .await?,
        )
    }
}
