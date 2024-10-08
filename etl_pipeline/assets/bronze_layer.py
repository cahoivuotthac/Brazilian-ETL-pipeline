import os
from dotenv import load_dotenv
import pandas as pd
from dagster import AssetExecutionContext, AssetIn, AssetOut, Definitions, MetadataValue, asset, Output, load_assets_from_modules, multi_asset

from resources import MINIO_CONFIG, MYSQL_CONFIG
from resources.minio_io_manager import MinIOIOManager
from resources.mysql_io_manager import MySQLIOManager

ls_tables = [
	"olist_order_items_dataset",
	"olist_order_payments_dataset",
	"olist_orders_dataset",
	"olist_products_dataset",
	"product_category_name_translation",
]

def create_bronze_assets(table_name):
	@asset(
		required_resource_keys={"mysql_io_manager"},
		name=f"bronze_{table_name}",
		io_manager_key="minio_io_manager",
		key_prefix=["bronze", "ecom"],
		compute_kind="MySQL",
		group_name="bronze"
	)
	def bronze_dataset(context: AssetExecutionContext) -> Output[pd.DataFrame]:
		sql_stm = f"SELECT * FROM {table_name}"
		pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
		if pd_data is None:
			raise ValueError("No data fetched from MySQL database")
		return Output(
			pd_data, 
			metadata={
				"table": MetadataValue.text(f"{table_name}"),
				"records count": MetadataValue.int(len(pd_data)),
			},
		)
	return bronze_dataset

bronze_assets = [create_bronze_assets(table) for table in ls_tables]

defs = Definitions(
	assets=bronze_assets,
	resources={
		"mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
		"minio_io_manager": MinIOIOManager(MINIO_CONFIG)
	},
)