//! Incident rules contain no I/O, clock reads, HTTP or database types.

pub const MAX_COUNTER: i64 = 9_007_199_254_740_991;
pub const MAX_SILENCE_MS: i64 = 86_400_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Open,
    Acknowledged,
    Resolved,
}

impl State {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Acknowledged => "acknowledged",
            Self::Resolved => "resolved",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Action {
    Acknowledge,
    Silence { until_ms: i64 },
    Resolve,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Head {
    pub state: State,
    pub revision: i64,
    pub silenced_until_ms: Option<i64>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Incident {
    pub id: String,
    pub dedup_key: String,
    pub service: String,
    pub kind: String,
    pub owner: String,
    pub severity: String,
    pub head: Head,
    pub occurrences: i64,
    pub first_seen_ms: i64,
    pub last_seen_ms: i64,
    pub resolved_at_ms: Option<i64>,
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum RuleError {
    #[error("revision_conflict")]
    Revision,
    #[error("invalid_transition")]
    Transition,
}

impl Head {
    pub fn apply(self, expected: i64, action: Action, now_ms: i64) -> Result<Self, RuleError> {
        if expected != self.revision || expected < 1 {
            return Err(RuleError::Revision);
        }
        if self.state == State::Resolved || self.revision >= MAX_COUNTER {
            return Err(RuleError::Transition);
        }
        let mut next = Self {
            revision: self.revision + 1,
            ..self
        };
        match action {
            Action::Acknowledge => next.state = State::Acknowledged,
            Action::Resolve => {
                next.state = State::Resolved;
                next.silenced_until_ms = None;
            }
            Action::Silence { until_ms } => {
                if until_ms <= now_ms || until_ms.saturating_sub(now_ms) > MAX_SILENCE_MS {
                    return Err(RuleError::Transition);
                }
                next.silenced_until_ms = Some(until_ms);
            }
        }
        Ok(next)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Condition {
    Failed,
    Recovered,
}

/// Only observed producer signals open or resolve incidents.
pub fn condition(
    event: &str,
    resource: Option<&str>,
    route: Option<&str>,
    status: Option<i32>,
) -> Option<(&'static str, Condition)> {
    match (event, resource, route, status) {
        ("admission.saturated", Some("connections"), _, _) => {
            Some(("admission:connections", Condition::Failed))
        }
        ("admission.saturated", Some("requests"), _, _) => {
            Some(("admission:requests", Condition::Failed))
        }
        ("admission.recovered", Some("connections"), _, _) => {
            Some(("admission:connections", Condition::Recovered))
        }
        ("admission.recovered", Some("requests"), _, _) => {
            Some(("admission:requests", Condition::Recovered))
        }
        ("database.readiness_failed", _, _, _) => Some(("database", Condition::Failed)),
        ("http.request.completed", _, Some("/v1/ready"), Some(200)) => {
            Some(("database", Condition::Recovered))
        }
        ("tls.reload.failed", _, _, _) => Some(("tls", Condition::Failed)),
        ("tls.reload.completed", _, _, _) => Some(("tls", Condition::Recovered)),
        ("process.failed" | "client.failure", _, _, _) => Some(("process", Condition::Failed)),
        ("server.started" | "client.ready", _, _, _) => Some(("process", Condition::Recovered)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acknowledgement_silence_and_resolution_preserve_revision_boundaries() {
        let head = Head {
            state: State::Open,
            revision: 1,
            silenced_until_ms: None,
        };
        assert_eq!(
            head.apply(0, Action::Acknowledge, 100).unwrap_err(),
            RuleError::Revision
        );
        let ack = head.apply(1, Action::Acknowledge, 100).unwrap();
        let silent = ack
            .apply(2, Action::Silence { until_ms: 200 }, 100)
            .unwrap();
        assert_eq!(silent.state, State::Acknowledged);
        assert_eq!(silent.silenced_until_ms, Some(200));
        let resolved = silent.apply(3, Action::Resolve, 150).unwrap();
        assert_eq!(resolved.state, State::Resolved);
        assert_eq!(resolved.silenced_until_ms, None);
        assert_eq!(
            resolved.apply(4, Action::Acknowledge, 150).unwrap_err(),
            RuleError::Transition
        );
        for until_ms in [100, 99, 100 + MAX_SILENCE_MS + 1, i64::MAX] {
            assert_eq!(
                head.apply(1, Action::Silence { until_ms }, 100)
                    .unwrap_err(),
                RuleError::Transition
            );
        }
    }

    #[test]
    fn unrelated_success_cannot_resolve_a_database_incident() {
        assert_eq!(
            condition("client.failure", None, None, None),
            Some(("process", Condition::Failed))
        );
        assert_eq!(condition("process.started", None, None, None), None);
        assert_eq!(condition("client.resumed", None, None, None), None);
        assert_eq!(
            condition("client.ready", None, None, None),
            Some(("process", Condition::Recovered))
        );
        assert_eq!(
            condition(
                "http.request.completed",
                None,
                Some("/v1/health"),
                Some(200)
            ),
            None
        );
        assert_eq!(
            condition("http.request.completed", None, Some("/v1/ready"), Some(200)),
            Some(("database", Condition::Recovered))
        );
        assert_eq!(
            condition("admission.recovered", Some("unknown"), None, None),
            None
        );
    }
}
