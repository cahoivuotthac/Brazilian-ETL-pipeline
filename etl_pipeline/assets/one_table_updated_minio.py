import os
from dotenv import load_dotenv
import pandas as pd
from dagster import asset, Output, Definitions, AssetIn
from resources.minio_io_manager import MinIOIOManager
from resources.mysql_io_manager import MySQLIOManager

load_dotenv()

@asset(
	io_manager_key="minio_io_manager",
	required_resource_keys={"mysql_io_manager"},
	key_prefix=["bronze", "ecom"],
	compute_kind="MySQL"
)
def bronze_olist_orders_dataset(context) -> Output[pd.DataFrame]:
	sql_stm = "SELECT * FROM olist_orders_dataset"
	pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
	if pd_data is None:
		raise ValueError("No data fetched from MySQL database")
	return Output(
		pd_data,
		metadata={
			"table": "olist_orders_dataset",
			"records count": len(pd_data),
		},
)

@asset(
	ins={
		"bronze_olist_orders_dataset": AssetIn(
			key_prefix=["bronze", "ecom"],
		)
	},
	compute_kind="PostgreSQL"
)
def olist_orders_dataset(bronze_olist_orders_dataset) -> Output[pd.DataFrame]:
	return Output(
		bronze_olist_orders_dataset,
		metadata={
			"schema": "public",
			"table": "bronze_olist_orders_dataset",
			"records counts": len(bronze_olist_orders_dataset),
		},
)

MYSQL_CONFIG = {
	"host": "localhost",
    "port": int(os.getenv("MYSQL_PORT")),
    "database": os.getenv("MYSQL_DATABASE"),
	"user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD")
}

MINIO_CONFIG = {
	"endpoint_url": "localhost:9000",
	"bucket": os.getenv("DATALAKE_BUCKET"),
	"aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
	"aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY")
}

defs = Definitions(
	assets=[bronze_olist_orders_dataset, olist_orders_dataset],
	resources={
		"mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
		"minio_io_manager": MinIOIOManager(MINIO_CONFIG)
	}
)