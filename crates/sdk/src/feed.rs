//! One caller-owned export feed of already validated/redacted events. This queue
//! is not durable; callers report delivery separately from local OS output.
use crate::Error;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, Weak},
};
const CAPACITY: usize = 128;
const MAX_COUNTER: u64 = 9_007_199_254_740_991;
#[derive(Clone, Copy, Default)]
pub struct FeedHealth {
    pub pending: u32,
    pub dropped: u64,
    pub delivered: u64,
    pub failures: u64,
}
#[derive(Default)]
struct State {
    items: VecDeque<Vec<u8>>,
    dropped: u64,
    delivered: u64,
    failures: u64,
    instance: String,
    sequence: u64,
}
pub struct EventFeed(Arc<Mutex<State>>);
#[derive(Default)]
pub(crate) struct Router(Mutex<Weak<Mutex<State>>>);
impl Router {
    pub fn subscribe(&self) -> Result<EventFeed, Error> {
        let mut slot = self.0.lock().map_err(|_| Error)?;
        if slot.upgrade().is_some() {
            return Err(Error);
        }
        let state = Arc::new(Mutex::new(State {
            instance: uuid::Uuid::new_v4().to_string(),
            ..State::default()
        }));
        *slot = Arc::downgrade(&state);
        Ok(EventFeed(state))
    }
    pub fn enqueue(&self, fields: &serde_json::Map<String, serde_json::Value>) {
        let queue = self.0.lock().ok().and_then(|slot| slot.upgrade());
        if let Some(queue) = queue {
            let mut queue = queue.lock().unwrap_or_else(|error| error.into_inner());
            if queue.sequence == MAX_COUNTER {
                queue.dropped = queue.dropped.saturating_add(1).min(MAX_COUNTER);
                return;
            }
            queue.sequence += 1;
            if queue.items.len() == CAPACITY {
                queue.dropped = queue.dropped.saturating_add(1).min(MAX_COUNTER);
            } else {
                let mut fields = fields.clone();
                fields.insert("producer.instance_id".into(), queue.instance.clone().into());
                fields.insert("producer.sequence".into(), queue.sequence.into());
                if let Ok(mut bytes) = serde_json::to_vec(&fields) {
                    bytes.push(b'\n');
                    queue.items.push_back(bytes);
                }
            }
        }
    }
}
impl EventFeed {
    /// Remove at most 32 complete NDJSON events / 64 KiB. The native sender owns
    /// this one batch until acknowledgement, expiry or cancellation.
    pub fn take_batch(&self) -> Option<(Vec<u8>, u32)> {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        let mut bytes = Vec::new();
        let mut count = 0;
        while count < 32
            && state
                .items
                .front()
                .is_some_and(|item| bytes.len() + item.len() <= 65_536)
        {
            bytes.extend(state.items.pop_front()?);
            count += 1;
        }
        (count > 0).then_some((bytes, count))
    }
    pub fn delivered(&self, count: u32) {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        state.delivered = state
            .delivered
            .saturating_add(u64::from(count))
            .min(MAX_COUNTER);
    }
    pub fn failed(&self) {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        state.failures = state.failures.saturating_add(1).min(MAX_COUNTER);
    }
    pub fn discard(&self, count: u32) {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        state.dropped = state
            .dropped
            .saturating_add(u64::from(count))
            .min(MAX_COUNTER);
    }
    pub fn health(&self) -> FeedHealth {
        let state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        FeedHealth {
            pending: state.items.len() as u32,
            dropped: state.dropped,
            delivered: state.delivered,
            failures: state.failures,
        }
    }
}
