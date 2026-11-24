# Pipeline de câmbio e juros com Apache Airflow

Pipeline de dados ponta a ponta com **Apache Airflow 3.4**, arquitetura **Medalhão**
(bronze, silver, gold) e orquestração orientada a **Assets**. Extrai a cotação PTAX
(câmbio) e a taxa Selic das APIs abertas do Banco Central e entrega métricas prontas
para consumo em um PostgreSQL.

## Visão geral

- **Fontes:** PTAX (API Olinda) e Selic diária, série 11 (API SGS), ambas do BCB
- **Destino:** PostgreSQL com os schemas `bronze`, `silver`, `gold` e `audit`
- **Orquestração:** Airflow 3.4.0, LocalExecutor, Docker Compose
- **Linguagens:** Python e SQL

## Arquitetura

A gold só roda quando **as duas** silvers geraram dados novos (agendamento por
múltiplos Assets). Dia sem cotação (fim de semana, feriado) não emite Asset, e a
camada seguinte não é acionada.

### Camadas

| Camada | Conteúdo | Tabelas |
|---|---|---|
| Bronze | Resposta original da API, em `jsonb`, mais arquivo em disco (landing zone) | `bronze.ptax_raw`, `bronze.selic_raw` |
| Silver | Dados tipados, validados e sem duplicatas; registros inválidos vão para quarentena | `silver.ptax_cotacao`, `silver.selic_taxa`, `silver.*_quarentena` |
| Gold | Métricas de consumo | `gold.ptax_diaria`, `gold.ptax_vs_selic_mensal` |
| Audit | Falhas, retries e sucessos das tasks | `audit.pipeline_eventos` |

## DAGs

| DAG | Gatilho | Função |
|---|---|---|
| `setup_dw` | Manual | Cria tabelas de bronze (PTAX) e de auditoria |
| `bronze_ptax_ingest` | Diária | Extrai o PTAX por moeda e grava arquivo e tabela raw |
| `selic_ingest` | Diária | Extrai a Selic e grava arquivo e tabela raw |
| `silver_ptax_transform` | Asset da bronze | Valida, isola inválidos e faz upsert |
| `silver_selic_transform` | Asset da bronze | Idem, para a Selic |
| `gold_ptax_metrics` | Dois Assets | Fechamento, variação, média móvel e comparativo mensal |
| `pipeline_reprocessamento` | Manual (com formulário) | Reprocessa um intervalo de datas |

<!-- Confira que a tabela reflete o que existe no seu projeto. -->

## Conceitos do Airflow demonstrados

| Conceito | Onde |
|---|---|
| TaskFlow API e operators clássicos | Todas as DAGs |
| Data interval e `CronDataIntervalTimetable` | Bronzes: a data processada vem de `data_interval_start` |
| Assets e agendamento por Assets | Bronze → silver → gold; gold depende de dois Assets |
| XCom | Apenas metadados (caminho, contagem), nunca o dado |
| Dynamic task mapping e task group | `ingestao_moeda`, uma trilha por moeda |
| Branch | `decidir_fluxo` separa dia com e sem cotação |
| `AirflowSkipException` | `selic_ingest`: dia vazio não emite Asset |
| Trigger rules | `none_failed_min_one_success` nos pontos de junção |
| Retries, backoff e timeouts | `extrair`, com `execution_timeout` e `dagrun_timeout` |
| Concorrência | `max_active_runs`, pool `api_bcb`, LocalExecutor |
| Sensores | <!-- se implementou: sensor da API em modo reschedule --> |
| Callbacks | Falha, retry e sucesso gravam em `audit.pipeline_eventos` |
| Connections e Variables | Definidas por variáveis de ambiente no compose |
| Params, `TriggerDagRunOperator`, deferrable | `pipeline_reprocessamento` (usa o triggerer) |
| `SQLExecuteQueryOperator` e Jinja | Silver e gold, com SQL em arquivos |

## Decisões de arquitetura

- **Lógica fora das DAGs.** Funções puras em `include/` (cliente HTTP, storage); as
  DAGs só orquestram. Isso permite testar sem Airflow.
- **Dois Postgres.** Metadados do Airflow separados dos dados do projeto.
- **Idempotência em toda carga.** Chaves naturais e `INSERT ... ON CONFLICT DO UPDATE`:
  retry, clear e backfill não duplicam dados.
- **Bronze guarda tudo.** `jsonb` com a resposta original, inclusive dias vazios, para
  distinguir "a API respondeu vazio" de "a task não rodou".
- **Qualidade em SQL.** As regras vivem em views (`vw_*_validacao`); a validação e o
  upsert usam a mesma definição de registro válido.
- **Escrita atômica de arquivos.** Arquivo temporário e `os.replace`.
- **Falha de auditoria não derruba a task.** Os callbacks capturam as próprias exceções.

## Como rodar

Pré-requisitos: Docker Desktop e Git.

```bash
git clone <url-do-repositorio>
cd airflow-ptax-pipeline
cp .env.example .env
```

Gere a `FERNET_KEY` e coloque no `.env`:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Suba o ambiente:

```bash
docker compose build
docker compose up airflow-init
docker compose up -d
```

1. Abra `http://localhost:8080` (usuário e senha padrão `airflow` / `airflow`, apenas
   para uso local).
2. Ative as DAGs e dispare a `setup_dw` uma vez.
3. Use `pipeline_reprocessamento` para carregar um intervalo histórico.

O DW fica acessível em `localhost:5433` (banco, usuário e senha do `.env`).

## Estrutura

```
dags/        # DAGs (orquestração)
include/     # lógica de negócio e SQL
  ptax/  sgs/  pipeline/  sql/{init,bronze,silver,gold,audit}/
tests/       # testes de funções puras e de integridade das DAGs
```

## Testes

```bash
uv run pytest tests/test_storage.py tests/test_sgs_client.py
docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests/test_dag_integrity.py
```