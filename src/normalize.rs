use crate::{
    application::{AcceptedEvent, Error},
    event_wire::{Resource, TelemetryEvent},
};
use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

pub struct Normalizer {
    schema: jsonschema::Validator,
}
impl Normalizer {
    pub fn new() -> Result<Self, Error> {
        let document: serde_json::Value =
            serde_json::from_str(include_str!("../contracts/telemetry-event.schema.json"))
                .map_err(|_| Error::Invalid)?;
        let schema = jsonschema::options()
            .with_draft(jsonschema::Draft::Draft202012)
            .should_validate_formats(true)
            .build(&document)
            .map_err(|_| Error::Invalid)?;
        Ok(Self { schema })
    }
    pub fn accept(
        &self,
        value: serde_json::Value,
        now: DateTime<Utc>,
    ) -> Result<(TelemetryEvent, AcceptedEvent), Error> {
        if !self.schema.is_valid(&value)
            || serde_json::to_vec(&value)
                .map_err(|_| Error::Invalid)?
                .len()
                > 16_384
        {
            return Err(Error::Invalid);
        }
        let mut event: TelemetryEvent =
            serde_json::from_value(value).map_err(|_| Error::Invalid)?;
        let time = DateTime::parse_from_rfc3339(&event.timestamp)
            .map_err(|_| Error::Invalid)?
            .with_timezone(&Utc);
        if time > now + chrono::Duration::seconds(60) || time < now - chrono::Duration::hours(24) {
            return Err(Error::Invalid);
        }
        // Error prose can include paths/provider data even under a safe field name.
        // Keep only bounded identifier-like classes; never forward free-form text.
        if event.error_type.as_ref().is_some_and(|v| {
            !v.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        }) {
            event.error_type = Some("redacted_error".into());
        }
        let payload = serde_json::to_vec(&event).map_err(|_| Error::Invalid)?;
        if payload.len() > 16_384 {
            return Err(Error::Invalid);
        }
        let result = AcceptedEvent {
            fingerprint: Sha256::digest(&payload).into(),
            payload,
            service: event.service_name.clone(),
            event_name: event.event_name.clone(),
            resource: event.resource.as_ref().map(|r| {
                match r {
                    Resource::Connections => "connections",
                    Resource::Requests => "requests",
                }
                .to_owned()
            }),
            route: event.route.clone(),
            status: event.status,
            timestamp_ms: time.timestamp_millis(),
            instance: event.producer_instance_id.clone(),
            sequence: event.producer_sequence,
        };
        Ok((event, result))
    }
}
