use crate::{
    application::{AcceptedEvent, Error},
    event_wire::{Resource, TelemetryEvent},
};
use chrono::{DateTime, Utc};
use nddev_device_sync_telemetry::decode_event;
use sha2::{Digest, Sha256};

pub fn accept(
    value: serde_json::Value,
    now: DateTime<Utc>,
) -> Result<(TelemetryEvent, AcceptedEvent), Error> {
    let event = decode_event(value).map_err(|_| Error::Invalid)?;
    let time = DateTime::parse_from_rfc3339(&event.timestamp)
        .map_err(|_| Error::Invalid)?
        .with_timezone(&Utc);
    if time > now + chrono::Duration::seconds(60) || time < now - chrono::Duration::hours(24) {
        return Err(Error::Invalid);
    }
    let payload = serde_json::to_vec(&event).map_err(|_| Error::Invalid)?;
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
