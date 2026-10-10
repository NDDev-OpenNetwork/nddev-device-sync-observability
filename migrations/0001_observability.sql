CREATE TABLE nds_observability_meta (key text PRIMARY KEY, value text NOT NULL);
INSERT INTO nds_observability_meta VALUES ('product','nddev-device-sync-observability'),('schema','1');

CREATE TABLE delivery_state (
    tenant_id text PRIMARY KEY,
    time_floor timestamptz NOT NULL DEFAULT clock_timestamp(),
    queued_events bigint NOT NULL DEFAULT 0 CHECK (queued_events BETWEEN 0 AND 4096),
    queued_bytes bigint NOT NULL DEFAULT 0 CHECK (queued_bytes BETWEEN 0 AND 67108864),
    delivered_events bigint NOT NULL DEFAULT 0,
    expired_events bigint NOT NULL DEFAULT 0,
    failures bigint NOT NULL DEFAULT 0,
    last_outcome text NOT NULL DEFAULT 'unknown' CHECK (last_outcome IN ('unknown','success','failure')),
    last_success_at timestamptz,
    last_failure_at timestamptz
);
CREATE TABLE event_receipts (
    tenant_id text NOT NULL REFERENCES delivery_state(tenant_id),
    event_id bytea NOT NULL CHECK (octet_length(event_id)=32),
    received_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    payload bytea,
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 8),
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    CHECK (payload IS NULL OR octet_length(payload)<=16384),
    PRIMARY KEY (tenant_id,event_id)
);
CREATE INDEX receipt_delivery ON event_receipts(next_attempt_at) WHERE payload IS NOT NULL;
CREATE INDEX receipt_retention ON event_receipts(received_at);

CREATE TABLE sources (
    tenant_id text NOT NULL REFERENCES delivery_state(tenant_id),
    service_name text NOT NULL CHECK (length(service_name) BETWEEN 1 AND 128),
    created_at timestamptz NOT NULL DEFAULT now(),
    last_received_at timestamptz,
    last_event_at timestamptz,
    instance_id uuid,
    last_sequence bigint CHECK (last_sequence BETWEEN 1 AND 9007199254740991),
    gap_observations bigint NOT NULL DEFAULT 0,
    gap_active boolean NOT NULL DEFAULT false,
    CHECK ((instance_id IS NULL)=(last_sequence IS NULL)),
    PRIMARY KEY (tenant_id,service_name)
);

CREATE TABLE incidents (
    tenant_id text NOT NULL REFERENCES delivery_state(tenant_id),
    incident_id text PRIMARY KEY,
    dedup_key text NOT NULL CHECK (length(dedup_key)<=256),
    service_name text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('admission','database','tls','process','delivery','source_stale','source_gap')),
    owner text NOT NULL DEFAULT 'operator' CHECK (owner='operator'),
    severity text NOT NULL CHECK (severity IN ('warning','critical')),
    state text NOT NULL CHECK (state IN ('open','acknowledged','resolved')),
    revision bigint NOT NULL CHECK (revision BETWEEN 1 AND 9007199254740991),
    occurrences bigint NOT NULL CHECK (occurrences BETWEEN 1 AND 9007199254740991),
    first_seen timestamptz NOT NULL,
    last_seen timestamptz NOT NULL,
    silenced_until timestamptz,
    resolved_at timestamptz,
    UNIQUE(tenant_id,dedup_key),
    UNIQUE(tenant_id,incident_id),
    FOREIGN KEY (tenant_id,service_name) REFERENCES sources(tenant_id,service_name)
);

CREATE TABLE incident_actions (
    tenant_id text NOT NULL REFERENCES delivery_state(tenant_id),
    operation_id text NOT NULL CHECK (length(operation_id) BETWEEN 1 AND 128),
    fingerprint bytea NOT NULL CHECK (octet_length(fingerprint)=32),
    incident_id text NOT NULL CHECK (length(incident_id) BETWEEN 1 AND 128),
    actor_id text NOT NULL CHECK (length(actor_id) BETWEEN 1 AND 128),
    action text NOT NULL CHECK (action IN ('acknowledge','silence','resolve')),
    result jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant_id,operation_id)
);
CREATE INDEX action_retention ON incident_actions(created_at);

-- Runtime cannot change schema evidence or SQLx migration history.
REVOKE INSERT,UPDATE,DELETE ON nds_observability_meta FROM nds_runtime;
REVOKE INSERT,UPDATE,DELETE ON _sqlx_migrations FROM nds_runtime;
