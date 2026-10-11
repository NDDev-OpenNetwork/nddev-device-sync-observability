//! One redaction projection for native spans, used by both export transports.
use crate::{Config, contract};
use opentelemetry::{KeyValue, Value};
use opentelemetry_sdk::{Resource, trace::SpanData};
use std::{collections::BTreeMap, sync::LazyLock};

static FIELDS: LazyLock<BTreeMap<&'static str, jsonschema::Validator>> = LazyLock::new(|| {
    ["module", "scope", "method", "route", "status"]
        .into_iter()
        .map(|key| {
            (
                key,
                jsonschema::validator_for(&contract::DOCUMENT["properties"][key])
                    .expect("embedded span field schema"),
            )
        })
        .collect()
});
pub(crate) fn valid_attribute(key: &str, value: &serde_json::Value) -> bool {
    let field = match key {
        "module" => "module",
        "scope" => "scope",
        "http.request.method" => "method",
        "http.route" => "route",
        "http.response.status_code" => "status",
        _ => return false,
    };
    let value = if field == "method" && value.as_str() == Some("_OTHER") {
        serde_json::json!("OTHER")
    } else {
        value.clone()
    };
    FIELDS[field].is_valid(&value)
}
pub(crate) fn project(span: &mut SpanData) -> bool {
    if !span.span_context.is_sampled()
        || span.attributes.iter().any(|field| {
            field.key.as_str() == "nds.export.suppressed" && field.value == Value::Bool(true)
        })
    {
        return false;
    }
    span.events.events.clear();
    span.links.links.clear();
    span.span_context = opentelemetry::trace::SpanContext::new(
        span.span_context.trace_id(),
        span.span_context.span_id(),
        span.span_context.trace_flags(),
        span.span_context.is_remote(),
        opentelemetry::trace::TraceState::default(),
    );
    if !matches!(span.name.as_ref(), "http.request" | "module.operation") {
        span.name = "module.operation".into();
    }
    span.attributes.retain(|field| {
        let value = match &field.value {
            Value::String(value) => serde_json::Value::String(value.to_string()),
            Value::I64(value) => (*value).into(),
            _ => return false,
        };
        valid_attribute(field.key.as_str(), &value)
    });
    if matches!(span.status, opentelemetry::trace::Status::Error { .. }) {
        span.status = opentelemetry::trace::Status::error("");
    }
    // Instrumentation metadata is also an export boundary, never arbitrary
    // library-provided attributes, version strings or schema URLs.
    span.instrumentation_scope =
        opentelemetry::InstrumentationScope::builder("nds-telemetry").build();
    true
}
pub(crate) fn resource(config: &Config) -> Resource {
    Resource::builder_empty()
        .with_attributes([
            KeyValue::new("service.name", config.service.clone()),
            KeyValue::new("service.version", config.version.clone()),
            KeyValue::new("source.repository", config.repository.clone()),
            KeyValue::new("source.commit", config.commit.clone()),
            KeyValue::new("deployment.environment.name", config.environment.clone()),
        ])
        .build()
}
