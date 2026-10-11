DO $$ BEGIN
    IF NOT EXISTS(SELECT 1 FROM nds_observability_meta WHERE key='schema' AND value='1') THEN
        RAISE EXCEPTION 'client streams require observability schema 1';
    END IF;
END $$;
ALTER TABLE sources ADD COLUMN required boolean NOT NULL DEFAULT true;
CREATE TABLE client_producers (
    tenant_id text NOT NULL REFERENCES delivery_state(tenant_id),
    instance_id uuid NOT NULL,
    service_name text NOT NULL CHECK(length(service_name) BETWEEN 1 AND 128),
    last_sequence bigint NOT NULL CHECK(last_sequence BETWEEN 1 AND 9007199254740991),
    last_received_at timestamptz NOT NULL,
    PRIMARY KEY(tenant_id,instance_id),
    FOREIGN KEY(tenant_id,service_name) REFERENCES sources(tenant_id,service_name)
);
UPDATE nds_observability_meta SET value='2' WHERE key='schema';
