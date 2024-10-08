# use this layer to ingest tables list from the source database (MySQL) to MinIO
import os
import sys 
from dagster import AssetIn, AssetOut, Definitions, Output, asset, multi_asset
import pandas as pd
import mysql.connector
import boto3
from dotenv import load_dotenv

from resources.minio_io_manager import MinIOIOManager
from resources.mysql_io_manager import MySQLIOManager
from resources.psql_io_manager import PostgreSQLIOManager
from week3.etl_pipeline.etl_pipeline.assets.configurations import MYSQL_CONFIG, MINIO_CONFIG, PSQL_CONFIG


tables = [
	"olist_order_items_dataset",
	"olist_order_payments_dataset",
	"olist_orders_dataset",
	"olist_products_dataset",
	"product_category_name_translation"
]

table_metadata = {
	"olist_order_items_dataset": {
		"primary_keys": ["order_id", "order_item_id"],
		"columns": ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"]
	},
	"olist_order_payments_dataset": {
		"primary_keys": ["order_id"],
		"columns": ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"]
	},
	"olist_orders_dataset": {
		"primary_keys": ["order_id"],
		"columns": ["order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"]
	},
	"olist_products_dataset": {
		"primary_keys": ["product_id"],
		"columns": ["product_id", "product_category_name", "product_name_length", "product_description_length", "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]
	},
	"product_category_name_translation": {
		"primary_keys": ["product_category_name"],
		"columns": ["product_category_name", "product_category_name_english"]
	}
}

for table_name in tables:
	@asset(
		name=f"bronze_{table_name}",
		required_resource_keys={"mysql_io_manager"},
		io_manager_key="minio_io_manager",
		key_prefix=["bronze", "ecom"],
		compute_kind="MySQL"
	)
	def bronze_ingest_table_to_minio(context) -> Output[pd.DataFrame]:
		query = f"SELECT * FROM {table_name}"
		pd_data = context.resources.mysql_io_manager.execute_data(query)
		return Output(
			pd_data,
			metadata={
				"table": table_name,
				"records count": len(pd_data),
			}
		)

	@multi_asset(
		ins={
				f"bronze_{table_name}": AssetIn(
        			key_prefix=["bronze", "ecom"]
           		)
		},
		outs={
			f"{table_name}": AssetOut(
				io_manager_key="psql_io_manager",
				key_prefix=["warehouse", "public"],
				metadata={
					"primary_keys": table_metadata[table_name]["primary_keys"],
					"columns": table_metadata[table_name]["columns"]
				}
			)
		},
		compute_kind="PostgreSQL"
	)
	def ingest_table_to_postgres(bronze_ingest_table_to_minio, table_name=table_name) -> Output[pd.DataFrame]:
		return Output(
			bronze_ingest_table_to_minio,
			metadata={
				"schema": "public",
				"table": f"bronze_{table_name}",
				"records count": len(bronze_ingest_table_to_minio)
			}
		)



