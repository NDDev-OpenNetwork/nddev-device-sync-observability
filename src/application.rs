//! Use-case data and ports. Concrete I/O belongs to adapters.
use crate::domain::{Action, Incident};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, thiserror::Error)]
pub enum Error {
    #[error("dependency_unavailable")]
    Unavailable,
    #[error("capacity_exhausted")]
    Capacity,
    #[error("invalid_request")]
    Invalid,
    #[error("not_found")]
    NotFound,
    #[error("revision_conflict")]
    Revision,
    #[error("idempotency_conflict")]
    Idempotency,
    #[error("invalid_transition")]
    Transition,
}

pub struct AcceptedEvent {
    pub fingerprint: [u8; 32],
    pub payload: Vec<u8>,
    pub service: String,
    pub event_name: String,
    pub resource: Option<String>,
    pub route: Option<String>,
    pub status: Option<i32>,
    pub timestamp_ms: i64,
    pub device: Option<String>,
    pub instance: Option<String>,
    pub sequence: Option<i64>,
}

pub struct Receipt {
    pub accepted: usize,
    pub duplicates: usize,
}

pub struct ActionCommand {
    pub incident: String,
    pub operation: String,
    pub actor: String,
    pub expected: i64,
    pub action: Action,
    pub fingerprint: [u8; 32],
}

pub trait Repository: Clone + Send + Sync + 'static {
    fn ingest(
        &self,
        events: &[AcceptedEvent],
        export: bool,
        ttl_seconds: u64,
    ) -> impl std::future::Future<Output = Result<Receipt, Error>> + Send;
    fn action(
        &self,
        command: ActionCommand,
    ) -> impl std::future::Future<Output = Result<Incident, Error>> + Send;
    fn incidents(
        &self,
        after: &str,
        limit: usize,
    ) -> impl std::future::Future<Output = Result<Vec<Incident>, Error>> + Send;
}
