INSERT INTO silver.ptax_cotacao (
    moeda, data_hora_cotacao, data_ref, tipo_boletim,
    cotacao_compra, cotacao_venda, paridade_compra, paridade_venda
)
SELECT DISTINCT ON (moeda, data_hora_cotacao)
    moeda, data_hora_cotacao, data_ref, tipo_boletim,
    cotacao_compra, cotacao_venda, paridade_compra, paridade_venda
FROM silver.vw_ptax_validacao
WHERE motivo_invalido IS NULL
ORDER BY moeda, data_hora_cotacao
ON CONFLICT (moeda, data_hora_cotacao) DO UPDATE SET
    data_ref        = EXCLUDED.data_ref,
    tipo_boletim    = EXCLUDED.tipo_boletim,
    cotacao_compra  = EXCLUDED.cotacao_compra,
    cotacao_venda   = EXCLUDED.cotacao_venda,
    paridade_compra = EXCLUDED.paridade_compra,
    paridade_venda  = EXCLUDED.paridade_venda,
    atualizado_em   = now();