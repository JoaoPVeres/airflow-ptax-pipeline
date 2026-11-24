INSERT INTO gold.ptax_diaria (
    moeda, data_ref, fechamento_compra, fechamento_venda,
    minima_venda, maxima_venda, qtd_boletins,
    variacao_dia_pct, media_movel_venda
)
WITH base AS (
    SELECT
        moeda,
        data_ref,
        MAX(cotacao_compra) FILTER (WHERE tipo_boletim = 'Fechamento PTAX') AS fechamento_compra,
        MAX(cotacao_venda)  FILTER (WHERE tipo_boletim = 'Fechamento PTAX') AS fechamento_venda,
        MIN(cotacao_venda)                                                  AS minima_venda,
        MAX(cotacao_venda)                                                  AS maxima_venda,
        COUNT(*)                                                            AS qtd_boletins
    FROM silver.ptax_cotacao
    GROUP BY moeda, data_ref
    HAVING COUNT(*) FILTER (WHERE tipo_boletim = 'Fechamento PTAX') > 0
)
SELECT
    moeda,
    data_ref,
    fechamento_compra,
    fechamento_venda,
    minima_venda,
    maxima_venda,
    qtd_boletins,
    ROUND(
        (fechamento_venda / LAG(fechamento_venda) OVER (PARTITION BY moeda ORDER BY data_ref) - 1) * 100,
        4
    ) AS variacao_dia_pct,
    CASE WHEN COUNT(*) OVER janela >= {{ params.janela_media }}
         THEN ROUND(AVG(fechamento_venda) OVER janela, 6)
    END AS media_movel_venda
FROM base
WINDOW janela AS (
    PARTITION BY moeda ORDER BY data_ref
    ROWS BETWEEN {{ params.janela_media - 1 }} PRECEDING AND CURRENT ROW
)
ON CONFLICT (moeda, data_ref) DO UPDATE SET
    fechamento_compra = EXCLUDED.fechamento_compra,
    fechamento_venda  = EXCLUDED.fechamento_venda,
    minima_venda      = EXCLUDED.minima_venda,
    maxima_venda      = EXCLUDED.maxima_venda,
    qtd_boletins      = EXCLUDED.qtd_boletins,
    variacao_dia_pct  = EXCLUDED.variacao_dia_pct,
    media_movel_venda = EXCLUDED.media_movel_venda,
    atualizado_em     = now();