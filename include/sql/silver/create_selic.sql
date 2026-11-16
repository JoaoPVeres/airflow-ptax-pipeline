CREATE TABLE IF NOT EXISTS silver.selic_taxa (
    serie            INTEGER        NOT NULL,
    data_taxa        DATE           NOT NULL,
    taxa_diaria_pct  NUMERIC(12,8)  NOT NULL,
    atualizado_em    TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (serie, data_taxa)
);

CREATE TABLE IF NOT EXISTS silver.selic_quarentena (
    serie          INTEGER      NOT NULL,
    data_ref       DATE         NOT NULL,
    item           JSONB        NOT NULL,
    motivo         TEXT         NOT NULL,
    registrado_em  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (serie, data_ref, item)
);

CREATE OR REPLACE VIEW silver.vw_selic_bronze_itens AS
SELECT
    b.serie,
    b.data_ref,
    i.item,
    CASE WHEN i.item ->> 'data' ~ '^\d{2}/\d{2}/\d{4}$'
         THEN to_date(i.item ->> 'data', 'DD/MM/YYYY') END   AS data_taxa,
    CASE WHEN i.item ->> 'valor' ~ '^-?\d+(\.\d+)?$'
         THEN (i.item ->> 'valor')::numeric END              AS taxa_diaria_pct
FROM bronze.selic_raw b
CROSS JOIN LATERAL jsonb_array_elements(b.payload) AS i(item);

CREATE OR REPLACE VIEW silver.vw_selic_validacao AS
SELECT
    v.*,
    NULLIF(CONCAT_WS('; ',
        CASE WHEN data_taxa IS NULL THEN 'data ausente ou invalida' END,
        CASE WHEN taxa_diaria_pct IS NULL THEN 'valor ausente ou nao numerico'
             WHEN taxa_diaria_pct < 0     THEN 'taxa negativa' END,
        CASE WHEN data_taxa IS NOT NULL AND data_taxa <> data_ref
             THEN 'data da taxa diferente do dia consultado' END
    ), '') AS motivo_invalido
FROM silver.vw_selic_bronze_itens v;