//! The protocol projection and redaction policy shared by producers and ingress.
use crate::{Error, event::TelemetryEvent};
use serde_json::Value;
use std::sync::LazyLock;

pub const MAX_EVENT_BYTES: usize = 16_384;
pub(crate) static DOCUMENT: LazyLock<Value> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../telemetry-event.schema.json"))
        .expect("embedded telemetry schema")
});
pub(crate) static VALIDATOR: LazyLock<jsonschema::Validator> = LazyLock::new(|| {
    jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .should_validate_formats(true)
        .build(&DOCUMENT)
        .expect("embedded telemetry contract")
});

/// Only stable classes are retained, never exception prose, paths or URLs.
pub fn error_class(value: &str) -> &str {
    if !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    {
        value
    } else {
        "redacted_error"
    }
}

/// Validate before decoding; generated DTOs alone do not enforce schema bounds.
/// Retention/freshness and transport admission remain with their owning adapter.
pub fn decode_event(mut value: Value) -> Result<TelemetryEvent, Error> {
    if !VALIDATOR.is_valid(&value)
        || serde_json::to_vec(&value).map_err(|_| Error)?.len() > MAX_EVENT_BYTES
    {
        return Err(Error);
    }
    if let Some(class) = value.get_mut("error.type") {
        *class = error_class(class.as_str().ok_or(Error)?).into();
    }
    serde_json::from_value(value).map_err(|_| Error)
}
