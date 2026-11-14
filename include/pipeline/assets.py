from airflow.sdk import Asset

# Definição central dos Assets: DAGs produtoras e consumidoras importam daqui,
# e o nome/URI fica num só lugar.
ASSET_BRONZE_PTAX = Asset(
    name="bronze_ptax_raw",
    uri="postgres://postgres-dw/dw/bronze/ptax_raw",
)

ASSET_SILVER_PTAX = Asset(
    name="silver_ptax_cotacao",
    uri="postgres://postgres-dw/dw/silver/ptax_cotacao",
)