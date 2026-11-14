CREATE TABLE IF NOT EXISTS silver.ptax_cotacao (
    moeda              VARCHAR(10)    NOT NULL,
    data_hora_cotacao  TIMESTAMP      NOT NULL,
    data_ref           DATE           NOT NULL,
    tipo_boletim       TEXT           NOT NULL,
    cotacao_compra     NUMERIC(14,6)  NOT NULL,
    cotacao_venda      NUMERIC(14,6)  NOT NULL,
    paridade_compra    NUMERIC(14,6),
    paridade_venda     NUMERIC(14,6),
    atualizado_em      TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (moeda, data_hora_cotacao)
);

CREATE TABLE IF NOT EXISTS silver.ptax_quarentena (
    moeda          VARCHAR(10)  NOT NULL,
    data_ref       DATE         NOT NULL,
    item           JSONB        NOT NULL,
    motivo         TEXT         NOT NULL,
    registrado_em  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (moeda, data_ref, item)
);

CREATE OR REPLACE VIEW silver.vw_ptax_bronze_itens AS
SELECT
    b.moeda,
    b.data_ref,
    i.item,
    CASE WHEN i.item ->> 'dataHoraCotacao' ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'
         THEN (i.item ->> 'dataHoraCotacao')::timestamp END                       AS data_hora_cotacao,
    i.item ->> 'tipoBoletim'                                                      AS tipo_boletim,
    CASE WHEN jsonb_typeof(i.item -> 'cotacaoCompra') = 'number'
         THEN (i.item ->> 'cotacaoCompra')::numeric END                           AS cotacao_compra,
    CASE WHEN jsonb_typeof(i.item -> 'cotacaoVenda') = 'number'
         THEN (i.item ->> 'cotacaoVenda')::numeric END                            AS cotacao_venda,
    CASE WHEN jsonb_typeof(i.item -> 'paridadeCompra') = 'number'
         THEN (i.item ->> 'paridadeCompra')::numeric END                          AS paridade_compra,
    CASE WHEN jsonb_typeof(i.item -> 'paridadeVenda') = 'number'
         THEN (i.item ->> 'paridadeVenda')::numeric END                           AS paridade_venda
FROM bronze.ptax_raw b
CROSS JOIN LATERAL jsonb_array_elements(b.payload -> 'value') AS i(item);

CREATE OR REPLACE VIEW silver.vw_ptax_validacao AS
SELECT
    v.*,
    NULLIF(CONCAT_WS('; ',
        CASE WHEN data_hora_cotacao IS NULL THEN 'data_hora_cotacao ausente ou invalida' END,
        CASE WHEN tipo_boletim IS NULL THEN 'tipo_boletim ausente' END,
        CASE WHEN cotacao_compra IS NULL OR cotacao_venda IS NULL THEN 'cotacao ausente ou nao numerica'
             WHEN cotacao_compra <= 0 OR cotacao_venda <= 0      THEN 'cotacao nao positiva'
             WHEN cotacao_venda < cotacao_compra                 THEN 'venda menor que compra' END,
        CASE WHEN data_hora_cotacao IS NOT NULL AND data_hora_cotacao::date <> data_ref
             THEN 'data da cotacao diferente do dia consultado' END
    ), '') AS motivo_invalido
FROM silver.vw_ptax_bronze_itens v;