CREATE TABLE IF NOT EXISTS gold.ptax_diaria (
    moeda              VARCHAR(10)    NOT NULL,
    data_ref           DATE           NOT NULL,
    fechamento_compra  NUMERIC(14,6)  NOT NULL,
    fechamento_venda   NUMERIC(14,6)  NOT NULL,
    minima_venda       NUMERIC(14,6)  NOT NULL,
    maxima_venda       NUMERIC(14,6)  NOT NULL,
    qtd_boletins       INTEGER        NOT NULL,
    variacao_dia_pct   NUMERIC(10,4),
    media_movel_venda  NUMERIC(14,6),
    atualizado_em      TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (moeda, data_ref)
);

CREATE TABLE IF NOT EXISTS gold.ptax_vs_selic_mensal (
    moeda                    VARCHAR(10)    NOT NULL,
    mes                      DATE           NOT NULL,
    ptax_fim_mes_venda       NUMERIC(14,6)  NOT NULL,
    variacao_mes_pct         NUMERIC(10,4),
    selic_acumulada_mes_pct  NUMERIC(12,6),
    dias_selic               INTEGER,
    atualizado_em            TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (moeda, mes)
);