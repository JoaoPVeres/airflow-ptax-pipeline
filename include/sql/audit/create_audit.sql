CREATE TABLE IF NOT EXISTS audit.pipeline_eventos (
    id             BIGSERIAL    PRIMARY KEY,
    dag_id         TEXT         NOT NULL,
    task_id        TEXT         NOT NULL,
    map_index      INTEGER,
    run_id         TEXT,
    evento         TEXT         NOT NULL,
    tentativa      INTEGER,
    mensagem       TEXT,
    registrado_em  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_pipeline_eventos_dag
    ON audit.pipeline_eventos (dag_id, registrado_em DESC);