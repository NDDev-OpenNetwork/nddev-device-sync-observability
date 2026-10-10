use crate::{CONTRACT, Error};
use opentelemetry::trace::Status;
use serde_json::Value;
use std::sync::LazyLock;
use tracing_opentelemetry::OpenTelemetrySpanExt;

static ROUTE: LazyLock<jsonschema::Validator> = LazyLock::new(|| {
    jsonschema::validator_for(&CONTRACT["properties"]["route"])
        .expect("embedded telemetry route schema")
});

/// Instrument the actual HTTP future. Pass a framework route template or
/// `unmatched`, never a raw URI. The method must be canonical (including OTHER).
/// Set the extracted parent before entering/polling the span or reading context.
/// This creates a real span; exporter ownership remains with the process.
pub fn http_server_span(method: &str, matched_route: &str) -> Result<tracing::Span, Error> {
    let properties = &CONTRACT["properties"];
    if !properties["method"]["enum"]
        .as_array()
        .expect("embedded method enum")
        .iter()
        .any(|value| value.as_str() == Some(method))
        || matched_route.len()
            > properties["route"]["maxLength"]
                .as_u64()
                .expect("embedded route bound") as usize
        || !ROUTE.is_valid(&Value::String(matched_route.into()))
    {
        return Err(Error);
    }
    let span = tracing::info_span!(target:"nds_telemetry", "http.request",
        otel.kind = "server", module = "http", scope = "http", method,
        route = matched_route, status = tracing::field::Empty);
    span.set_attribute(
        "http.request.method",
        if method == "OTHER" { "_OTHER" } else { method }.to_owned(),
    );
    if matched_route != "unmatched" {
        span.set_attribute("http.route", matched_route.to_owned());
    }
    Ok(span)
}

/// Call once before the completion event and before dropping the request span.
/// HTTP 4xx does not mark a server span as failed; 5xx does, without error prose.
pub fn finish_http_server_span(span: &tracing::Span, status: u16) -> Result<(), Error> {
    if !(100..=599).contains(&status) {
        return Err(Error);
    }
    span.record("status", status);
    span.set_attribute("http.response.status_code", i64::from(status));
    if status >= 500 {
        span.set_status(Status::error(""));
    }
    Ok(())
}
