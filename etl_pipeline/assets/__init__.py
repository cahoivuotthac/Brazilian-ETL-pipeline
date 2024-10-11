from dagster import Definitions, load_assets_from_modules

from resources import MINIO_CONFIG, MYSQL_CONFIG, PSQL_CONFIG
from assets import bronze_layer
from assets import silver_layer
from assets import gold_layer
from assets import warehouse_layer

all_assets = load_assets_from_modules(
    [
		bronze_layer, 
		silver_layer, 
		gold_layer, 
		warehouse_layer
    ]
)

defs = Definitions(
    assets=all_assets,
    resources={
        "psql_io_manager": PSQL_CONFIG,
        "minio_io_manager": MINIO_CONFIG,
        "mysql_io_manager": MYSQL_CONFIG,
    }
)