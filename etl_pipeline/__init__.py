from dagster import Definitions, load_assets_from_modules

from resources import MINIO_CONFIG, MYSQL_CONFIG
from assets import bronze_layer, silver_layer

all_assets = load_assets_from_modules(bronze_layer, silver_layer)

defs = Definitions(
	assets=all_assets,
	resources={
		"mysql_io_manager": MYSQL_CONFIG,
		"minio_io_manager": MINIO_CONFIG,
	}
)