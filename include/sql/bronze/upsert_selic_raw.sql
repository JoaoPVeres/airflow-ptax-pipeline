INSERT INTO bronze.selic_raw (
    serie, data_ref, payload, qtd_registros, source_url, arquivo_origem, run_id
)
VALUES (
    %(serie)s,
    %(data_ref)s,
    %(payload)s::jsonb,
    %(qtd_registros)s,
    %(source_url)s,
    %(arquivo_origem)s,
    %(run_id)s
)
ON CONFLICT (serie, data_ref) DO UPDATE SET
    payload        = EXCLUDED.payload,
    qtd_registros  = EXCLUDED.qtd_registros,
    source_url     = EXCLUDED.source_url,
    arquivo_origem = EXCLUDED.arquivo_origem,
    run_id         = EXCLUDED.run_id,
    ingested_at    = now();