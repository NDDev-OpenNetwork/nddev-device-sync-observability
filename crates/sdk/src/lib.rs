//! Process-owned, bounded telemetry. No dependency events, bodies or message dumps.
use opentelemetry::{
    Context,
    trace::{TraceContextExt, TracerProvider as _},
};
use opentelemetry_sdk::trace::{SdkTracerProvider, Tracer};
use serde_json::{Map, Value};
use std::{
    fmt,
    io::Write,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tracing::{
    Event, Level, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{
    Layer,
    layer::{Context as LayerContext, SubscriberExt},
    registry::LookupSpan,
};

mod output;
pub use output::{LocalWriter, OutputHealth};

pub const MAX_EVENT_BYTES: usize = 16_384;
const MAX_COUNTER: u64 = 9_007_199_254_740_991;
const SCHEMA: &str = include_str!("../telemetry-event.schema.json");

#[derive(Debug, thiserror::Error)]
#[error("invalid telemetry configuration or unavailable subscriber")]
pub struct Error;

/// Immutable public build identity. Never pass operator identities or credentials.
#[derive(Clone)]
pub struct Config {
    pub service: String,
    pub version: String,
    pub repository: String,
    pub commit: String,
    pub channel: String,
    pub standards: String,
    pub environment: String,
    pub target_prefixes: Vec<String>,
    pub default_module: String,
    pub debug: Option<DebugWindow>,
}
#[derive(Clone)]
pub struct DebugWindow {
    pub scope: String,
    pub duration: Duration,
    pub events: u64,
}

struct Policy {
    schema: jsonschema::Validator,
    properties: Map<String, Value>,
    base: Map<String, Value>,
}
impl Policy {
    fn new(config: &Config) -> Result<Self, Error> {
        if config.target_prefixes.is_empty()
            || config.target_prefixes.len() > 16
            || config.target_prefixes.iter().any(|v| !identifier(v))
        {
            return Err(Error);
        }
        if let Some(debug) = &config.debug
            && (!valid_scope(&debug.scope)
                || !(1..=900).contains(&debug.duration.as_secs())
                || debug.duration.subsec_nanos() != 0
                || !(1..=10000).contains(&debug.events))
        {
            return Err(Error);
        }
        let source: Value = serde_json::from_str(SCHEMA).map_err(|_| Error)?;
        let schema = jsonschema::options()
            .should_validate_formats(true)
            .build(&source)
            .map_err(|_| Error)?;
        let base: Map<_, _> = [
            ("service.name", &config.service),
            ("service.version", &config.version),
            ("release.version", &config.version),
            ("source.repository", &config.repository),
            ("source.commit", &config.commit),
            ("release.channel", &config.channel),
            ("standards.release", &config.standards),
            ("deployment.environment", &config.environment),
            ("module", &config.default_module),
        ]
        .into_iter()
        .map(|(k, v)| (k.into(), Value::String(v.clone())))
        .collect();
        let mut check = base.clone();
        check.insert("timestamp".into(), chrono::Utc::now().to_rfc3339().into());
        check.insert("severity".into(), "info".into());
        check.insert("event.name".into(), "logging.initialized".into());
        if !schema.is_valid(&Value::Object(check)) {
            return Err(Error);
        }
        Ok(Self {
            schema,
            properties: source["properties"].as_object().ok_or(Error)?.clone(),
            base,
        })
    }
    fn writable(&self, key: &str) -> bool {
        self.properties.contains_key(key)
            && !self.base.contains_key(key)
            && !matches!(
                key,
                "timestamp"
                    | "severity"
                    | "producer.instance_id"
                    | "producer.sequence"
                    | "trace_id"
                    | "span_id"
            )
            || key == "module"
    }
    fn max_string(&self, key: &str) -> usize {
        self.properties
            .get(key)
            .and_then(|v| v.get("maxLength"))
            .and_then(Value::as_u64)
            .unwrap_or(128) as usize
    }
}
fn valid_scope(scope: &str) -> bool {
    matches!(scope, "http" | "transport" | "database" | "io" | "process")
}
fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.:-".contains(&b))
}

#[derive(Default, Clone)]
struct Fields {
    values: Map<String, Value>,
    rejected: u64,
}
struct Visitor<'a> {
    fields: &'a mut Fields,
    policy: &'a Policy,
}
impl Visitor<'_> {
    fn insert(&mut self, field: &Field, value: Value) {
        if self.policy.writable(field.name()) {
            self.fields.values.insert(field.name().into(), value);
        } else {
            self.fields.rejected += 1;
        }
    }
    fn text(&mut self, field: &Field, text: &str) {
        if !self.policy.writable(field.name()) {
            self.fields.rejected += 1;
            return;
        }
        if text.len() > self.policy.max_string(field.name()) {
            self.fields.rejected += 1;
            return;
        }
        let text = if field.name() == "error.type" && !identifier(text) {
            "redacted_error"
        } else {
            text
        };
        self.insert(field, text.into());
    }
}
struct BoundedText {
    value: String,
    limit: usize,
}
impl fmt::Write for BoundedText {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        if self.value.len() + value.len() > self.limit {
            return Err(fmt::Error);
        }
        self.value.push_str(value);
        Ok(())
    }
}
impl Visit for Visitor<'_> {
    fn record_str(&mut self, f: &Field, v: &str) {
        self.text(f, v)
    }
    fn record_bool(&mut self, f: &Field, v: bool) {
        self.insert(f, v.into())
    }
    fn record_i64(&mut self, f: &Field, v: i64) {
        self.insert(f, v.into())
    }
    fn record_u64(&mut self, f: &Field, v: u64) {
        self.insert(f, v.into())
    }
    fn record_f64(&mut self, f: &Field, v: f64) {
        self.insert(f, Value::from(v))
    }
    fn record_debug(&mut self, f: &Field, v: &dyn fmt::Debug) {
        // Crucially do not even format unknown fields: Debug may expose secrets
        // or allocate an arbitrarily large body before redaction can run.
        if !self.policy.writable(f.name()) {
            self.fields.rejected += 1;
            return;
        }
        let mut text = BoundedText {
            value: String::new(),
            limit: self.policy.max_string(f.name()),
        };
        if fmt::write(&mut text, format_args!("{v:?}")).is_err() {
            self.fields.rejected += 1;
        } else {
            self.text(f, &text.value);
        }
    }
}

struct Sink<W> {
    writer: W,
    sequence: u64,
    instance: String,
}
struct ActiveDebug {
    window: DebugWindow,
    expires: Instant,
    remaining: u64,
}
struct Shared<W> {
    policy: Policy,
    sink: Mutex<Sink<W>>,
    rejected: AtomicU64,
    write_failures: AtomicU64,
    debug: Mutex<Option<ActiveDebug>>,
    config: Config,
}
impl<W: Write> Shared<W> {
    fn emit(&self, mut fields: Map<String, Value>, severity: &str) {
        for (key, value) in &self.policy.base {
            if key != "module" || !fields.contains_key(key) {
                fields.insert(key.clone(), value.clone());
            }
        }
        fields.insert(
            "timestamp".into(),
            chrono::Utc::now()
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                .into(),
        );
        fields.insert("severity".into(), severity.into());
        if !self.policy.schema.is_valid(&Value::Object(fields.clone())) {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            fields.retain(|k, _| {
                self.policy.base.contains_key(k) || matches!(k.as_str(), "timestamp" | "severity")
            });
            fields.insert("event.name".into(), "logging.event.rejected".into());
            fields.insert("module".into(), "process".into());
            fields.insert("severity".into(), "warn".into());
            fields.insert("outcome".into(), "rejected".into());
            fields.insert("rejected_count".into(), 1.into());
        }
        let Ok(mut sink) = self.sink.lock() else {
            self.write_failures.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if sink.sequence < MAX_COUNTER {
            sink.sequence += 1;
            fields.insert("producer.instance_id".into(), sink.instance.clone().into());
            fields.insert("producer.sequence".into(), sink.sequence.into());
        }
        let Ok(mut bytes) = serde_json::to_vec(&fields) else {
            return;
        };
        if bytes.len() > MAX_EVENT_BYTES {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return;
        }
        bytes.push(b'\n');
        if sink.writer.write_all(&bytes).is_err() {
            self.write_failures.fetch_add(1, Ordering::Relaxed);
        }
    }
    fn reject_fields(&self, count: u64) {
        if count > 0 {
            self.rejected.fetch_add(count, Ordering::Relaxed);
            self.emit(
                Map::from_iter([
                    ("event.name".into(), "logging.fields.rejected".into()),
                    ("module".into(), "process".into()),
                    ("rejected_count".into(), count.min(MAX_COUNTER).into()),
                    ("outcome".into(), "rejected".into()),
                ]),
                "warn",
            );
        }
    }
    fn debug_notice(&self, debug: &DebugWindow, reason: Option<&str>) {
        let mut fields = Map::from_iter([
            (
                "event.name".into(),
                if reason.is_some() {
                    "logging.debug.disabled"
                } else {
                    "logging.debug.enabled"
                }
                .into(),
            ),
            ("scope".into(), debug.scope.clone().into()),
            ("outcome".into(), "ok".into()),
        ]);
        if let Some(reason) = reason {
            fields.insert("reason".into(), reason.into());
        } else {
            fields.insert("duration_seconds".into(), debug.duration.as_secs().into());
            fields.insert("event_budget".into(), debug.events.into());
        }
        self.emit(fields, "info");
    }
    fn disable_debug(&self, reason: &str) {
        if let Ok(mut active) = self.debug.lock()
            && let Some(debug) = active.take()
        {
            self.debug_notice(&debug.window, Some(reason));
        }
    }
    fn debug_tick(&self) {
        if let Ok(mut active) = self.debug.lock()
            && let Some(debug) = active.as_ref()
        {
            let reason = if Instant::now() >= debug.expires {
                Some("expired")
            } else if debug.remaining == 0 {
                Some("event_budget")
            } else {
                None
            };
            if let Some(reason) = reason {
                let debug = active.take().expect("present window");
                self.debug_notice(&debug.window, Some(reason));
            }
        }
    }
    fn admit_debug(&self, module: &str) -> bool {
        self.debug_tick();
        if let Ok(mut active) = self.debug.lock()
            && let Some(debug) = active.as_mut()
            && debug.window.scope == module
            && debug.remaining > 0
        {
            debug.remaining -= 1;
            return true;
        }
        false
    }
}
struct EventLayer<W> {
    shared: Arc<Shared<W>>,
    dispatch: std::sync::OnceLock<tracing::dispatcher::WeakDispatch>,
}
impl<S, W> Layer<S> for EventLayer<W>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    W: Write + Send + Sync + 'static,
{
    fn on_register_dispatch(&self, dispatch: &tracing::Dispatch) {
        let _ = self.dispatch.set(dispatch.downgrade());
    }
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: LayerContext<'_, S>) {
        if let Some(span) = ctx.span(id) {
            let mut fields = Fields::default();
            attrs.record(&mut Visitor {
                fields: &mut fields,
                policy: &self.shared.policy,
            });
            span.extensions_mut().insert(fields);
        }
    }
    fn on_record(&self, id: &Id, record: &Record<'_>, ctx: LayerContext<'_, S>) {
        if let Some(span) = ctx.span(id)
            && let Some(fields) = span.extensions_mut().get_mut::<Fields>()
        {
            record.record(&mut Visitor {
                fields,
                policy: &self.shared.policy,
            });
        }
    }
    fn on_event(&self, event: &Event<'_>, ctx: LayerContext<'_, S>) {
        let metadata = event.metadata();
        let owned = self.shared.config.target_prefixes.iter().any(|prefix| {
            metadata.target() == prefix
                || metadata
                    .target()
                    .strip_prefix(prefix)
                    .is_some_and(|rest| rest.starts_with("::"))
        });
        if !owned {
            return;
        }
        let mut fields = Fields::default();
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope.from_root() {
                if let Some(value) = span.extensions().get::<Fields>() {
                    fields.values.extend(value.values.clone());
                    fields.rejected += value.rejected;
                }
            }
        }
        event.record(&mut Visitor {
            fields: &mut fields,
            policy: &self.shared.policy,
        });
        let scope = fields
            .values
            .get("scope")
            .and_then(Value::as_str)
            .unwrap_or("process");
        if *metadata.level() > Level::INFO
            && (*metadata.level() != Level::DEBUG || !self.shared.admit_debug(scope))
        {
            return;
        }
        if let Some(span) = ctx.event_span(event) {
            let context = self
                .dispatch
                .get()
                .and_then(|weak| weak.upgrade())
                .and_then(|dispatch| {
                    tracing_opentelemetry::get_otel_context(&span.id(), &dispatch)
                });
            if let Some(context) = context {
                let span = context.span();
                let id = span.span_context();
                if id.is_valid() {
                    fields
                        .values
                        .insert("trace_id".into(), id.trace_id().to_string().into());
                    fields
                        .values
                        .insert("span_id".into(), id.span_id().to_string().into());
                }
            }
        }
        self.shared.reject_fields(fields.rejected);
        self.shared.emit(
            fields.values,
            &metadata.level().as_str().to_ascii_lowercase(),
        );
        self.shared.debug_tick();
    }
}

/// Handle reports local write/rejection failures independently of external delivery.
pub struct Control<W: Write> {
    shared: Arc<Shared<W>>,
    output: Option<Arc<output::State>>,
}
impl<W: Write> Clone for Control<W> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            output: self.output.clone(),
        }
    }
}
impl<W: Write> Control<W> {
    pub fn rejected_fields_or_events(&self) -> u64 {
        self.shared.rejected.load(Ordering::Relaxed)
    }
    pub fn write_failures(&self) -> u64 {
        self.output_health().write_failures
    }
    pub fn output_health(&self) -> OutputHealth {
        let mut status = self
            .output
            .as_ref()
            .map_or_else(OutputHealth::default, |state| state.snapshot());
        status.write_failures = status
            .write_failures
            .saturating_add(self.shared.write_failures.load(Ordering::Relaxed));
        status
    }
    pub fn tick(&self) {
        self.shared.debug_tick();
    }
    pub fn debug_active(&self) -> bool {
        self.tick();
        self.shared
            .debug
            .lock()
            .is_ok_and(|window| window.is_some())
    }
    /// Replace the diagnostic window atomically; None returns to normal mode.
    pub fn set_debug(&self, window: Option<DebugWindow>) -> Result<(), Error> {
        if self.output_health().terminating {
            return Err(Error);
        }
        if let Some(debug) = &window
            && (!valid_scope(&debug.scope)
                || !(1..=900).contains(&debug.duration.as_secs())
                || debug.duration.subsec_nanos() != 0
                || !(1..=10000).contains(&debug.events))
        {
            return Err(Error);
        }
        let mut active = self.shared.debug.lock().map_err(|_| Error)?;
        if let Some(previous) = active.take() {
            self.shared.debug_notice(&previous.window, Some("operator"));
        }
        if let Some(window) = window {
            self.shared.debug_notice(&window, None);
            *active = Some(ActiveDebug {
                expires: Instant::now() + window.duration,
                remaining: window.events,
                window,
            });
        }
        Ok(())
    }
}
/// Caller-owned writer and provider: this embedded/test API has the writer's own
/// blocking/lifecycle contract. Process entry points use bounded `install`.
pub fn subscriber<W: Write + Send + Sync + 'static>(
    config: Config,
    writer: W,
    tracer: Tracer,
) -> Result<(impl Subscriber + Send + Sync, Control<W>), Error> {
    let policy = Policy::new(&config)?;
    let shared = Arc::new(Shared {
        policy,
        sink: Mutex::new(Sink {
            writer,
            sequence: 0,
            instance: uuid::Uuid::new_v4().to_string(),
        }),
        rejected: AtomicU64::new(0),
        write_failures: AtomicU64::new(0),
        debug: Mutex::new(config.debug.clone().map(|window| ActiveDebug {
            expires: Instant::now() + window.duration,
            remaining: window.events,
            window,
        })),
        config,
    });
    let otel = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
            metadata.is_span() && metadata.target() == "nds_telemetry"
        }));
    let subscriber = tracing_subscriber::registry().with(otel).with(EventLayer {
        dispatch: std::sync::OnceLock::new(),
        shared: shared.clone(),
    });
    if let Some(debug) = &shared.config.debug {
        shared.emit(
            Map::from_iter([
                ("event.name".into(), "logging.debug.enabled".into()),
                ("scope".into(), debug.scope.clone().into()),
                ("duration_seconds".into(), debug.duration.as_secs().into()),
                ("event_budget".into(), debug.events.into()),
                ("outcome".into(), "ok".into()),
            ]),
            "info",
        );
    }
    Ok((
        subscriber,
        Control {
            shared,
            output: None,
        },
    ))
}

pub struct Logger {
    pub control: Control<LocalWriter>,
    worker: Option<output::Worker>,
    provider: Option<SdkTracerProvider>,
    expiry: Option<(std::sync::mpsc::Sender<()>, std::thread::JoinHandle<()>)>,
}
impl Drop for Logger {
    fn drop(&mut self) {
        if let Some((cancel, thread)) = self.expiry.take() {
            let _ = cancel.send(());
            let _ = thread.join();
        }
        self.control.shared.disable_debug("process_exit");
        if let Some(provider) = &self.provider {
            let _ = provider.shutdown_with_timeout(Duration::from_secs(2));
        }
        if let Some(mut worker) = self.worker.take() {
            worker.close();
        }
    }
}
/// Local structured output and real spans; no OTLP exporter is configured here.
pub fn install(config: Config) -> Result<Logger, Error> {
    let provider = SdkTracerProvider::builder().build();
    let tracer = provider.tracer("nds-telemetry");
    let mut logger = install_with_tracer(config, tracer)?;
    logger.provider = Some(provider);
    Ok(logger)
}
pub fn install_with_tracer(config: Config, tracer: Tracer) -> Result<Logger, Error> {
    static INSTALLED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    INSTALLED.set(()).map_err(|_| Error)?;
    let (writer, worker) = output::start().map_err(|_| Error)?;
    let output = writer.state.clone();
    let (subscriber, mut control) = subscriber(config, writer, tracer)?;
    control.output = Some(output.clone());
    tracing::subscriber::set_global_default(subscriber).map_err(|_| Error)?;
    let shared = control.shared.clone();
    let (cancel, receiver) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        let mut heartbeat = Instant::now();
        while matches!(
            receiver.recv_timeout(Duration::from_secs(1)),
            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
        ) {
            shared.debug_tick();
            if heartbeat.elapsed() >= Duration::from_secs(30) {
                shared.emit(
                    Map::from_iter([
                        ("event.name".into(), "process.heartbeat".into()),
                        ("module".into(), "process".into()),
                        (
                            "outcome".into(),
                            if output.snapshot().dropped_events > 0 {
                                "error"
                            } else {
                                "ok"
                            }
                            .into(),
                        ),
                        (
                            "dropped_count".into(),
                            output.snapshot().dropped_events.into(),
                        ),
                    ]),
                    if output.snapshot().dropped_events > 0 {
                        "warn"
                    } else {
                        "info"
                    },
                );
                heartbeat = Instant::now();
            }
        }
    });
    let expiry = Some((cancel, thread));
    Ok(Logger {
        control,
        provider: None,
        worker: Some(worker),
        expiry,
    })
}

/// A real operation span with a validated, bounded module label. Instrument the
/// actual future with this span; do not create spans from previously emitted logs.
pub fn operation_span(module: &str, scope: &str) -> Result<tracing::Span, Error> {
    if !identifier(module) || !valid_scope(scope) {
        return Err(Error);
    }
    Ok(tracing::info_span!(target:"nds_telemetry","module.operation",module,scope))
}
/// Used by transport owners with a W3C TextMapPropagator; excludes all secrets.
pub fn trace_context(span: &tracing::Span) -> Context {
    span.context()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;
    #[derive(Clone)]
    struct Capture(Arc<Mutex<Vec<u8>>>);
    impl Write for Capture {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    fn config() -> Config {
        Config {
            service: "generated-sdk-acceptance".into(),
            version: "0.0.1-alpha.1".into(),
            repository: "NDDev-OpenNetwork/nddev-device-sync-observability".into(),
            commit: "unknown".into(),
            channel: "alpha".into(),
            standards: "v0.0.1-alpha.9".into(),
            environment: "test".into(),
            target_prefixes: vec!["nddev_device_sync_telemetry".into()],
            default_module: "process".into(),
            debug: None,
        }
    }
    fn records(bytes: &Arc<Mutex<Vec<u8>>>) -> Vec<Value> {
        let schema: Value = serde_json::from_str(SCHEMA).unwrap();
        let validator = jsonschema::options()
            .should_validate_formats(true)
            .build(&schema)
            .unwrap();
        let result: Vec<Value> = bytes
            .lock()
            .unwrap()
            .split(|b| *b == b'\n')
            .filter(|b| !b.is_empty())
            .map(|line| {
                assert!(line.len() <= MAX_EVENT_BYTES);
                serde_json::from_slice(line).unwrap()
            })
            .collect();
        for value in &result {
            assert!(validator.is_valid(value), "schema rejected emitted event");
        }
        result
    }
    #[test]
    fn unknown_debug_is_not_formatted_and_schema_violations_are_observable() {
        struct MustNotFormat;
        impl fmt::Debug for MustNotFormat {
            fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
                panic!("unknown secret-bearing field was formatted")
            }
        }
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let provider = SdkTracerProvider::builder().build();
        let (subscriber, control) =
            subscriber(config(), Capture(bytes.clone()), provider.tracer("test")).unwrap();
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!(event.name="operation.completed",outcome="ok",body=?MustNotFormat);
            tracing::info!(event.name = "invalid.bound", status = 600);
            tracing::info!(event.name="operation.failed",error.type="private free prose must not leave the process",outcome="error");
        });
        let values = records(&bytes);
        assert_eq!(values.len(), 4);
        assert_eq!(values[0]["event.name"], "logging.fields.rejected");
        assert_eq!(values[1]["event.name"], "operation.completed");
        assert_eq!(values[2]["event.name"], "logging.event.rejected");
        assert_eq!(values[3]["error.type"], "redacted_error");
        assert_eq!(control.rejected_fields_or_events(), 2);
    }
    #[test]
    fn actual_nested_sdk_spans_correlate_events_without_export_claim() {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let provider = SdkTracerProvider::builder().build();
        let (subscriber, _control) =
            subscriber(config(), Capture(bytes.clone()), provider.tracer("test")).unwrap();
        tracing::subscriber::with_default(subscriber, || {
            let parent = operation_span("agent", "io").unwrap();
            let _parent = parent.enter();
            tracing::info!(event.name = "operation.started", outcome = "started");
            let child = operation_span("sysinfo", "io").unwrap();
            assert!(trace_context(&child).span().span_context().is_valid());
            let _child = child.enter();
            tracing::info!(event.name = "operation.completed", outcome = "ok");
        });
        let values = records(&bytes);
        assert_eq!(values.len(), 2);
        assert_eq!(values[0]["trace_id"], values[1]["trace_id"]);
        assert_ne!(values[0]["span_id"], values[1]["span_id"]);
        assert_eq!(values[1]["module"], "sysinfo");
    }
    #[test]
    fn concurrent_writes_are_complete_ordered_and_debug_budget_is_finite() {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let provider = SdkTracerProvider::builder().build();
        let mut config = config();
        config.debug = Some(DebugWindow {
            scope: "io".into(),
            duration: Duration::from_secs(30),
            events: 2,
        });
        let (subscriber, control) =
            subscriber(config, Capture(bytes.clone()), provider.tracer("test")).unwrap();
        let dispatch = tracing::Dispatch::new(subscriber);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let dispatch = &dispatch;
                scope.spawn(move || {
                    tracing::dispatcher::with_default(dispatch, || {
                        for _ in 0..8 {
                            tracing::info!(event.name = "parallel.completed", outcome = "ok");
                            tracing::debug!(
                                event.name = "diagnostic.completed",
                                module = "sysinfo",
                                scope = "io",
                                outcome = "ok"
                            );
                        }
                    })
                });
            }
        });
        let values = records(&bytes);
        assert_eq!(
            values
                .iter()
                .filter(|v| v["event.name"] == "parallel.completed")
                .count(),
            64
        );
        assert_eq!(
            values
                .iter()
                .filter(|v| v["event.name"] == "diagnostic.completed")
                .count(),
            2
        );
        assert_eq!(
            values
                .iter()
                .filter(|v| v["event.name"] == "logging.debug.disabled")
                .count(),
            1
        );
        for (index, value) in values.iter().enumerate() {
            assert_eq!(value["producer.sequence"], (index + 1) as u64);
        }
        assert_eq!(control.write_failures(), 0);
    }
    #[test]
    fn runtime_debug_toggle_uses_one_window_and_shared_budget() {
        let bytes = Arc::new(Mutex::new(Vec::new()));
        let provider = SdkTracerProvider::builder().build();
        let (subscriber, control) =
            subscriber(config(), Capture(bytes.clone()), provider.tracer("test")).unwrap();
        tracing::subscriber::with_default(subscriber, || {
            assert!(!control.debug_active());
            control
                .set_debug(Some(DebugWindow {
                    scope: "http".into(),
                    duration: Duration::from_secs(10),
                    events: 1,
                }))
                .unwrap();
            assert!(control.debug_active());
            tracing::debug!(event.name = "wrong.scope", module = "process");
            tracing::debug!(
                event.name = "request.completed",
                module = "accounts",
                scope = "http",
                outcome = "ok"
            );
            assert!(!control.debug_active());
            control
                .set_debug(Some(DebugWindow {
                    scope: "http".into(),
                    duration: Duration::from_secs(10),
                    events: 5,
                }))
                .unwrap();
            control.set_debug(None).unwrap();
            tracing::debug!(
                event.name = "after.disable",
                module = "accounts",
                scope = "http"
            );
        });
        let values = records(&bytes);
        assert_eq!(values.len(), 5);
        assert_eq!(values[2]["reason"], "event_budget");
        assert_eq!(values[4]["reason"], "operator");
    }
    #[test]
    fn io_failure_and_invalid_configuration_are_reported() {
        struct Fails;
        impl Write for Fails {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::Error::other("generated I/O failure"))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let provider = SdkTracerProvider::builder().build();
        let (subscriber, control) = subscriber(config(), Fails, provider.tracer("test")).unwrap();
        tracing::subscriber::with_default(subscriber, || {
            tracing::error!(event.name = "process.failed", outcome = "error")
        });
        assert_eq!(control.write_failures(), 1);
        assert!(operation_span("bad module", "io").is_err());
        let mut invalid = config();
        invalid.debug = Some(DebugWindow {
            scope: "all".into(),
            duration: Duration::from_secs(30),
            events: 1,
        });
        assert!(Policy::new(&invalid).is_err());
    }
}
