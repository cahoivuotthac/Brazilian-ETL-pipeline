from dagster import AssetExecutionContext, AssetIn, AssetOut, Definitions, MetadataValue, Output, load_assets_from_modules, multi_asset

import pandas as pd
from resources import MINIO_CONFIG, MYSQL_CONFIG, PSQL_CONFIG
from assets import bronze_layer, silver_layer, gold_layer

from assets.bronze_layer import bronze_olist_order_items_dataset, bronze_olist_order_payments_dataset, bronze_olist_orders_dataset, bronze_olist_products_dataset, bronze_product_category_name_translation
from assets.gold_layer import gold_sales_values_by_category
from assets.silver_layer import dim_products, fact_sales

GROUP_NAME="warehouse_layer"

@multi_asset(
	ins={
		"gold_sales_values_by_category": AssetIn(
			key_prefix=["gold", "ecom"],
		)
	},
	outs={
		"sales_values_by_category": AssetOut(
			io_manager_key="psql_io_manager",
			key_prefix=["warehouse", "gold"],
			metadata={
				"primary_keys": "product_category_name_english",
				"columns": [
					"product_category_name_english",
					"sales_value"
				],
			}
		)
	},
	compute_kind="Postgres",
	group_name=GROUP_NAME
)
def sales_values_by_category(
	context: AssetExecutionContext,
	gold_sales_values_by_category: pd.DataFrame
) -> Output[pd.DataFrame]:
	return Output(
		value=gold_sales_values_by_category,
		metadata={
			"schema": MetadataValue.text("public"),
			"table": MetadataValue.text("sales_values_by_category"),
			"records count": MetadataValue.int(len(gold_sales_values_by_category))
		}
	)
