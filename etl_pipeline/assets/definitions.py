from dagster import Definitions

from etl_pipeline.assets.bronze_layer import bronze_olist_orders_dataset
from etl_pipeline.resources import MINIO_CONFIG, MYSQL_CONFIG
from resources.minio_io_manager import MinIOIOManager
from resources.mysql_io_manager import MySQLIOManager

defs = Definitions(
	assets=[bronze_olist_orders_dataset],
	resources={
		"mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
		"minio_io_manager": MinIOIOManager(MINIO_CONFIG)
	},
)