#![cfg(feature = "otlp")]
use nddev_device_sync_telemetry::{
    Config, finish_http_server_span, http_server_span, otlp::NativeTraces,
};
use opentelemetry::{
    Context,
    trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState},
};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::atomic::Ordering,
    time::Duration,
};
use tracing_opentelemetry::OpenTelemetrySpanExt;

fn config() -> Config {
    Config {
        service: "nds-otlp-acceptance".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        repository: "NDDev-OpenNetwork/nddev-device-sync-observability".into(),
        commit: "unknown".into(),
        channel: "alpha".into(),
        standards: "v0.0.1-alpha.9".into(),
        environment: "test".into(),
        target_prefixes: vec!["nds_acceptance".into()],
        default_module: "process".into(),
        debug: None,
    }
}

#[test]
fn actual_http_otlp_retries_and_exports_only_projected_native_spans() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/", listener.local_addr().unwrap());
    let receiver = std::thread::spawn(move || {
        let mut bodies = Vec::new();
        for response in ["503 Service Unavailable", "200 OK"] {
            let (mut socket, _) = listener.accept().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut bytes = Vec::new();
            let (header_size, length) = loop {
                let mut chunk = [0; 4096];
                let count = socket.read(&mut chunk).unwrap();
                assert!(count > 0 && bytes.len() + count < 128 * 1024);
                bytes.extend_from_slice(&chunk[..count]);
                if let Some(end) = bytes.windows(4).position(|value| value == b"\r\n\r\n") {
                    let header = std::str::from_utf8(&bytes[..end]).unwrap();
                    assert!(header.starts_with("POST /v1/traces HTTP/1.1"));
                    let length = header
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .map(str::to_owned)
                        })
                        .unwrap()
                        .parse::<usize>()
                        .unwrap();
                    break (end + 4, length);
                }
            };
            while bytes.len() < header_size + length {
                let mut chunk = [0; 4096];
                let count = socket.read(&mut chunk).unwrap();
                assert!(count > 0 && bytes.len() + count < 128 * 1024);
                bytes.extend_from_slice(&chunk[..count]);
            }
            bodies.push(bytes[header_size..header_size + length].to_vec());
            write!(
                socket,
                "HTTP/1.1 {response}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
            )
            .unwrap();
        }
        assert_eq!(bodies[0], bodies[1]);
        ExportTraceServiceRequest::decode(bodies[1].as_slice()).unwrap()
    });
    let native = NativeTraces::new(&config(), &endpoint).unwrap();
    let (subscriber, _) =
        nddev_device_sync_telemetry::subscriber(config(), std::io::sink(), native.tracer())
            .unwrap();
    tracing::subscriber::with_default(subscriber, || {
        let span = http_server_span("GET", "/v1/health").unwrap();
        span.set_parent(Context::new().with_remote_span_context(SpanContext::new(
            TraceId::from_bytes([7; 16]),
            SpanId::from_bytes([8; 8]),
            TraceFlags::SAMPLED,
            true,
            TraceState::from_key_value([("private", "PRIVATE_TRACE_VALUE")]).unwrap(),
        )))
        .unwrap();
        span.in_scope(|| {
            tracing::event!(target:"nds_acceptance",tracing::Level::INFO,{event.name="acceptance.completed",outcome="ok"});
            finish_http_server_span(&span, 200).unwrap();
        });
    });
    native.flush().unwrap();
    let received = receiver.join().unwrap();
    let spans = &received.resource_spans[0].scope_spans[0].spans;
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].name, "http.request");
    assert!(spans[0].trace_state.is_empty());
    assert_eq!(spans[0].trace_id, vec![7; 16]);
    assert_eq!(spans[0].parent_span_id, vec![8; 8]);
    assert_eq!(spans[0].kind, 2); // Native SERVER span, not a reconstructed log.
    assert!(spans[0].end_time_unix_nano > spans[0].start_time_unix_nano);
    assert!(spans[0].events.is_empty() && spans[0].links.is_empty());
    assert!(spans[0].attributes.iter().all(|attribute| matches!(
        attribute.key.as_str(),
        "module" | "scope" | "http.request.method" | "http.route" | "http.response.status_code"
    )));
    assert_eq!(native.delivery.successes.load(Ordering::Relaxed), 1);
    assert_eq!(native.delivery.failures.load(Ordering::Relaxed), 0);
    assert_eq!(native.delivery.pending.load(Ordering::Relaxed), 0);
    assert!(native.delivery.last_success_ms.load(Ordering::Relaxed) > 0);
}

#[test]
fn actual_unresponsive_collector_cannot_grow_the_queue_or_hide_loss() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}/", listener.local_addr().unwrap());
    let native = NativeTraces::new(&config(), &endpoint).unwrap();
    let (subscriber, _) =
        nddev_device_sync_telemetry::subscriber(config(), std::io::sink(), native.tracer())
            .unwrap();
    tracing::subscriber::with_default(subscriber, || {
        for _ in 0..1000 {
            let _span = http_server_span("GET", "/v1/health").unwrap();
        }
    });
    assert!(native.delivery.pending.load(Ordering::Relaxed) <= 128);
    assert!(native.delivery.dropped.load(Ordering::Relaxed) >= 872);
    drop(listener);
    native.flush().unwrap();
    assert_eq!(native.delivery.pending.load(Ordering::Relaxed), 0);
    assert!(native.delivery.failures.load(Ordering::Relaxed) > 0);
    assert_eq!(native.delivery.dropped.load(Ordering::Relaxed), 1000);
}
