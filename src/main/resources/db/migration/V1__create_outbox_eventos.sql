-- Primera migración Flyway: incorpora tablas de mensajería al esquema existente.
CREATE TABLE IF NOT EXISTS outbox_eventos (
    event_id VARCHAR(36) PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    aggregate_id VARCHAR(100) NOT NULL,
    partition_key VARCHAR(256) NOT NULL,
    payload TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    sent_at TIMESTAMP WITH TIME ZONE,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMP WITH TIME ZONE NOT NULL,
    last_error VARCHAR(1000)
);
CREATE INDEX IF NOT EXISTS idx_outbox_pendientes ON outbox_eventos (sent_at, next_attempt_at, created_at);
CREATE TABLE IF NOT EXISTS eventos_kinesis_procesados (
    event_id VARCHAR(100) PRIMARY KEY,
    procesado_en TIMESTAMP WITH TIME ZONE NOT NULL
);
