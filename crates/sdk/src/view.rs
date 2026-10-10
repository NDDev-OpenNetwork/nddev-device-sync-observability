//! Typed local diagnostics shared with native presentation. No raw error input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DebugScope {
    Http,
    Transport,
    Database,
    Io,
    Process,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DiagnosticsError {
    #[error("invalid_diagnostics_input")]
    InvalidInput,
    #[error("diagnostics_unavailable")]
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailureKind {
    FlutterFramework,
    DartUnhandled,
    NativeBridge,
}
impl FailureKind {
    pub(crate) fn class(self) -> &'static str {
        match self {
            Self::FlutterFramework => "flutter_framework_error",
            Self::DartUnhandled => "dart_unhandled_error",
            Self::NativeBridge => "native_bridge_error",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientLifecycle {
    Ready,
    Resumed,
    Inactive,
    Hidden,
    Paused,
    Detached,
}
impl ClientLifecycle {
    pub(crate) fn event(self) -> &'static str {
        match self {
            Self::Ready => "client.ready",
            Self::Resumed => "client.resumed",
            Self::Inactive => "client.inactive",
            Self::Hidden => "client.hidden",
            Self::Paused => "client.paused",
            Self::Detached => "client.detached",
        }
    }
}

/// A local sink is never evidence of remote collection or searchable delivery.
#[derive(Clone)]
pub struct LocalDiagnostics {
    pub sink_initialized: bool,
    pub debug_enabled: bool,
    pub debug_scope: Option<DebugScope>,
    pub rejected_fields_or_events: Option<u64>,
    pub dropped_events: Option<u64>,
    pub pending_events: Option<u64>,
    pub write_failures: Option<u64>,
    pub terminating: Option<bool>,
    pub shutdown_incomplete: Option<bool>,
}
