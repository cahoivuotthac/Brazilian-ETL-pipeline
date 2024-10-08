
from dagster import Definitions

from assets.one_table import bronze_olist_orders_dataset
from etl_pipeline.resources import MINIO_CONFIG, MYSQL_CONFIG, PSQL_CONFIG

defs = Definitions(
    assets=[bronze_olist_orders_dataset],
    resources={
        "mysql_io_manager": MYSQL_CONFIG,
        "minio_io_manager": MINIO_CONFIG,
        "psql_io_manager": PSQL_CONFIG
    }
)