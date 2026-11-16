INSERT INTO silver.selic_quarentena (serie, data_ref, item, motivo)
SELECT serie, data_ref, item, motivo_invalido
FROM silver.vw_selic_validacao
WHERE motivo_invalido IS NOT NULL
ON CONFLICT (serie, data_ref, item) DO UPDATE
SET motivo = EXCLUDED.motivo,
    registrado_em = now();