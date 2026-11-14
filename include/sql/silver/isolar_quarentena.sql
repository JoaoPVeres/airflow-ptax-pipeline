INSERT INTO silver.ptax_quarentena (moeda, data_ref, item, motivo)
SELECT moeda, data_ref, item, motivo_invalido
FROM silver.vw_ptax_validacao
WHERE motivo_invalido IS NOT NULL
ON CONFLICT (moeda, data_ref, item) DO UPDATE
SET motivo = EXCLUDED.motivo,
    registrado_em = now();