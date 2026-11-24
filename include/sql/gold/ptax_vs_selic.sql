INSERT INTO gold.ptax_vs_selic_mensal (
    moeda, mes, ptax_fim_mes_venda, variacao_mes_pct,
    selic_acumulada_mes_pct, dias_selic
)
WITH ptax_mes AS (
    SELECT DISTINCT ON (moeda, date_trunc('month', data_ref))
        moeda,
        date_trunc('month', data_ref)::date AS mes,
        fechamento_venda                    AS ptax_fim_mes_venda
    FROM gold.ptax_diaria
    ORDER BY moeda, date_trunc('month', data_ref), data_ref DESC
),
selic_mes AS (
    SELECT
        date_trunc('month', data_taxa)::date                         AS mes,
        (EXP(SUM(LN(1 + taxa_diaria_pct / 100))) - 1) * 100          AS selic_acumulada_mes_pct,
        COUNT(*)                                                     AS dias_selic
    FROM silver.selic_taxa
    GROUP BY 1
)
SELECT
    p.moeda,
    p.mes,
    p.ptax_fim_mes_venda,
    ROUND(
        (p.ptax_fim_mes_venda / LAG(p.ptax_fim_mes_venda) OVER (PARTITION BY p.moeda ORDER BY p.mes) - 1) * 100,
        4
    ),
    ROUND(s.selic_acumulada_mes_pct, 6),
    s.dias_selic
FROM ptax_mes p
LEFT JOIN selic_mes s ON s.mes = p.mes
ON CONFLICT (moeda, mes) DO UPDATE SET
    ptax_fim_mes_venda      = EXCLUDED.ptax_fim_mes_venda,
    variacao_mes_pct        = EXCLUDED.variacao_mes_pct,
    selic_acumulada_mes_pct = EXCLUDED.selic_acumulada_mes_pct,
    dias_selic              = EXCLUDED.dias_selic,
    atualizado_em           = now();