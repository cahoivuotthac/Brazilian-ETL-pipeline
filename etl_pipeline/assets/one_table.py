import os
from dotenv import load_dotenv
import pandas as pd
from dagster import AssetExecutionContext, Definitions, MetadataValue, asset, Output

from resources.minio_io_manager import MinIOIOManager
from resources.mysql_io_manager import MySQLIOManager
load_dotenv()

@asset(
	required_resource_keys={"mysql_io_manager"},
	io_manager_key="minio_io_manager",
	key_prefix=["bronze", "ecom"],
	compute_kind="MySQL"
)
def bronze_olist_orders_dataset(context: AssetExecutionContext) -> Output[pd.DataFrame]:
	sql_stm = "SELECT * FROM olist_orders_dataset"
	pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
	if pd_data is None:
		raise ValueError("No data fetched from MySQL database")
	return Output(
		pd_data, 
		metadata={
			"table": MetadataValue.text("olist_orders_dataset"),
			"records count": MetadataValue.int(len(pd_data)),
		},
	)

MYSQL_CONFIG = {
	"host": os.getenv("MYSQL_HOST"),
	"port": os.getenv("MYSQL_PORT"),
	"database": os.getenv("MYSQL_DATABASE"),
	"user": os.getenv("MYSQL_USER"),
	"password": os.getenv("MYSQL_PASSWORD")
}

MINIO_CONFIG = {
	"endpoint_url": os.getenv("MINIO_ENDPOINT"),
	"bucket": os.getenv("DATALAKE_BUCKET"),
	"aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
	"aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY")
}

defs = Definitions(
	assets=[bronze_olist_orders_dataset],
	resources={
		"mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
		"minio_io_manager": MinIOIOManager(MINIO_CONFIG)
	},
)