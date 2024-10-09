from dagster import AssetIn, AssetOut, Definitions, MetadataValue, Output, asset, multi_asset
import pandas as pd

from assets.bronze_layer import bronze_olist_order_items_dataset, bronze_olist_order_payments_dataset, bronze_olist_orders_dataset, bronze_olist_products_dataset, bronze_product_category_name_translation
from resources import MINIO_CONFIG, MYSQL_CONFIG

GROUP_NAME="silver_layer"

@multi_asset(
	ins={
		"bronze_olist_products_dataset": AssetIn(
			key_prefix=["bronze", "ecom"]
		),
		"bronze_product_category_name_translation": AssetIn(
			key_prefix=["bronze", "ecom"]
		)
	},
	outs={
		"dim_products": AssetOut(
			io_manager_key="minio_io_manager",
			key_prefix=["silver", "ecom"],
			metadata={
				"primary_keys": "product_id",
				"columns": [
					"product_id", 
					"product_category_name_english"
				],
			},
		),
	},
	group_name=GROUP_NAME,
	compute_kind="MinIO"
)
def dim_products(
    bronze_olist_products_dataset: pd.DataFrame,
    bronze_product_category_name_translation: pd.DataFrame
) -> Output[pd.DataFrame]:
    
    merged_data = pd.merge(
		bronze_olist_products_dataset,
		bronze_product_category_name_translation,
		on="product_category_name"
	)
    
    dim_products_data = merged_data[["product_id", "product_category_name_english"]]
 
    return Output(
		value=dim_products_data,
		metadata={
			"table": MetadataValue.text("dim_products"),
            "records count": MetadataValue.int(len(dim_products_data)),
		}
	)

@multi_asset(
	ins={
		"bronze_olist_order_items_dataset": AssetIn(
			key_prefix=["bronze", "ecom"]
		),
		"bronze_olist_order_payments_dataset": AssetIn(
			key_prefix=["bronze", "ecom"]
		),
		"bronze_olist_orders_dataset": AssetIn(
			key_prefix=["bronze", "ecom"]
		)
	},
	outs={
		"fact_sales": AssetOut(
			io_manager_key="minio_io_manager",
			key_prefix=["silver", "ecom"],
			metadata={
				"primary_keys": "order_id",
				"columns": [
					"order_id",
					"customer_id",
					"payment_value",
					"order_purchase_timestamp",
					"product_id",
					"order_status"
				], 
			}
		)
	},
	group_name=GROUP_NAME,
	compute_kind="MinIO"
)
def fact_sales(
    bronze_olist_order_items_dataset: pd.DataFrame,
    bronze_olist_order_payments_dataset: pd.DataFrame,
    bronze_olist_orders_dataset
) -> Output[pd.DataFrame]:
    
    merged_data = pd.merge(
        bronze_olist_orders_dataset,
        bronze_olist_order_items_dataset,
        on="order_id"
    )
    
    merged_data = pd.merge(
        merged_data,
        bronze_olist_order_payments_dataset,
        on="order_id"
    )
    
    fact_sales_data = merged_data[[
		"order_id",
		"customer_id",
		"payment_value",
		"order_purchase_timestamp",
		"product_id",
		"order_status"
	]]
    
    return Output(
		value=fact_sales_data,
		metadata={
			"table": MetadataValue.text("fact_sales"),
			"records count": MetadataValue.int(len(fact_sales_data)),
		}
	)

defs = Definitions(
	assets=[
		dim_products,
		fact_sales,
		bronze_olist_products_dataset,
		bronze_product_category_name_translation,
		bronze_olist_order_items_dataset,
		bronze_olist_order_payments_dataset,
		bronze_olist_orders_dataset
	],
	resources={
		"minio_io_manager": MINIO_CONFIG,
		"mysql_io_manager": MYSQL_CONFIG
	}
)