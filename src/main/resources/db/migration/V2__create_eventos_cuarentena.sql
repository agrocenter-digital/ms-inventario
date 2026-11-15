CREATE TABLE eventos_kinesis_cuarentena (
    id VARCHAR(64) PRIMARY KEY,
    stream VARCHAR(128) NOT NULL,
    shard VARCHAR(128) NOT NULL,
    sequence_number VARCHAR(128) NOT NULL,
    payload_base64 TEXT NOT NULL,
    error_tipo VARCHAR(200) NOT NULL,
    intentos INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL
);
