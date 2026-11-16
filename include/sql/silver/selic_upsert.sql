INSERT INTO silver.selic_taxa (serie, data_taxa, taxa_diaria_pct)
SELECT DISTINCT ON (serie, data_taxa)
    serie, data_taxa, taxa_diaria_pct
FROM silver.vw_selic_validacao
WHERE motivo_invalido IS NULL
ORDER BY serie, data_taxa
ON CONFLICT (serie, data_taxa) DO UPDATE SET
    taxa_diaria_pct = EXCLUDED.taxa_diaria_pct,
    atualizado_em   = now();