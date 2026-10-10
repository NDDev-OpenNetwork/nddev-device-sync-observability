//! Generated, owned input for process-output acceptance; no operator data.
use nddev_device_sync_telemetry::{Config, DebugWindow, install, operation_span};
use std::time::Duration;
fn main() {
    let mut logger = install(Config {
        service: "sdk-acceptance".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        repository: "NDDev-OpenNetwork/nddev-device-sync-observability".into(),
        commit: "unknown".into(),
        channel: "alpha".into(),
        standards: "v0.0.1-alpha.9".into(),
        environment: "test".into(),
        target_prefixes: vec!["sdk_probe".into()],
        default_module: "process".into(),
        debug: Some(DebugWindow {
            scope: "io".into(),
            duration: Duration::from_secs(1),
            events: 100,
        }),
    })
    .expect("owned valid config");
    if std::env::args().nth(1).as_deref() == Some("--panic") {
        panic!("SYNTHETIC_PRIVATE_PANIC /private/example token=do-not-emit");
    }
    if std::env::args().nth(1).as_deref() == Some("--shutdown-race") {
        let control = logger.control.clone();
        let barrier = std::sync::Barrier::new(9);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    barrier.wait();
                    for _ in 0..4096 {
                        tracing::info!(event.name = "race.operation", outcome = "ok");
                    }
                });
            }
            barrier.wait();
            logger.shutdown();
        });
        logger.shutdown(); // A repeated controlled exit emits no second terminal event.
        let health = control.output_health();
        assert!(health.terminating && !health.shutdown_incomplete);
        assert_eq!(health.pending_events, 0);
        eprintln!(
            "{{\"dropped\":{},\"pending\":0,\"attempted\":32772}}",
            health.dropped_events
        );
        return;
    }
    if std::env::args().nth(1).as_deref() == Some("--blocked-stdout") {
        for _ in 0..4096 {
            tracing::warn!(event.name = "owned.operation.completed", outcome = "ok");
        }
        let control = logger.control.clone();
        let before = control.output_health();
        assert!(before.dropped_events > 0);
        let started = std::time::Instant::now();
        drop(logger);
        let elapsed = started.elapsed().as_millis();
        let after = control.output_health();
        assert!(after.terminating && after.shutdown_incomplete);
        assert!(elapsed < 2500);
        let dropped = after.dropped_events;
        tracing::info!(event.name = "after.shutdown", outcome = "ok");
        assert!(control.output_health().dropped_events > dropped);
        eprintln!(
            "{{\"dropped\":{},\"shutdown_ms\":{},\"terminated\":true}}",
            dropped, elapsed
        );
        return;
    }
    for kind in [
        nddev_device_sync_telemetry::view::FailureKind::FlutterFramework,
        nddev_device_sync_telemetry::view::FailureKind::DartUnhandled,
        nddev_device_sync_telemetry::view::FailureKind::NativeBridge,
    ] {
        logger.control.report_failure(kind);
    }
    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                let span = operation_span("sysinfo", "io").expect("valid module");
                let _entered = span.enter();
                for _ in 0..4 {
                    tracing::info!(event.name = "operation.completed", outcome = "ok");
                }
                tracing::debug!(event.name = "diagnostic.completed", outcome = "ok");
            });
        }
    });
    std::thread::sleep(Duration::from_millis(1100));
    tracing::debug!(
        event.name = "diagnostic.after_expiry",
        module = "sysinfo",
        scope = "io",
        outcome = "ok"
    );
    tracing::info!(event.name = "process.completed", outcome = "ok");
}
