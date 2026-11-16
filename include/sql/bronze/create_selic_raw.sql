CREATE TABLE IF NOT EXISTS bronze.selic_raw (
    serie           INTEGER      NOT NULL,
    data_ref        DATE         NOT NULL,
    payload         JSONB        NOT NULL,
    qtd_registros   INTEGER      NOT NULL,
    source_url      TEXT,
    arquivo_origem  TEXT,
    run_id          TEXT,
    ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (serie, data_ref)
);