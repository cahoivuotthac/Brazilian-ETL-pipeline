from dagster import AssetExecutionContext, AssetIn, AssetOut, Definitions, MetadataValue, Output, asset, multi_asset
import pandas as pd

from assets.bronze_layer import bronze_olist_order_items_dataset, bronze_olist_order_payments_dataset, bronze_olist_orders_dataset, bronze_olist_products_dataset, bronze_product_category_name_translation
from resources import MINIO_CONFIG, MYSQL_CONFIG

GROUP_NAME="silver_layer"

@multi_asset(
	ins={
		"bronze_olist_products_dataset": AssetIn(
			key_prefix=["bronze", "ecom"],
		),
		"bronze_product_category_name_translation": AssetIn(
			key_prefix=["bronze", "ecom"],
		)
	},
	outs={
		"dim_products": AssetOut(
			name="dim_products",
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
    context: AssetExecutionContext,
	bronze_olist_products_dataset: pd.DataFrame,
    bronze_product_category_name_translation: pd.DataFrame
) -> Output[pd.DataFrame]:
    
    bronze_product_category_name_translation = bronze_product_category_name_translation["product_category_name"]
    bronze_olist_products_dataset = bronze_olist_products_dataset[["product_id", "product_category_name"]]
    
    dim_products_data = pd.merge(
		bronze_product_category_name_translation,
		bronze_olist_products_dataset,
		on="product_category_name"
	)
    
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
			key_prefix=["bronze", "ecom"],
		),
		"bronze_olist_order_payments_dataset": AssetIn(
			key_prefix=["bronze", "ecom"],
		),
		"bronze_olist_orders_dataset": AssetIn(
			key_prefix=["bronze", "ecom"],
		)
	},
	outs={
		"fact_sales": AssetOut(
			name="fact_sales",
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
    context: AssetExecutionContext,
    bronze_olist_order_items_dataset,
	bronze_olist_order_payments_dataset,
	bronze_olist_orders_dataset
) -> Output[pd.DataFrame]:
    
    bronze_olist_order_items_dataset = bronze_olist_order_items_dataset[["product_id", "order_id"]]
    bronze_olist_order_payments_dataset = bronze_olist_order_payments_dataset[["order_id", "payment_value"]]
    bronze_olist_orders_dataset = bronze_olist_orders_dataset[["order_id", "customer_id", "order_purchase_timestamp", "order_status"]]
    
    merged_data = pd.merge(
        bronze_olist_orders_dataset,
        bronze_olist_order_items_dataset,
        on="order_id"
    )
    
    fact_sales_data = pd.merge(
        merged_data,
        bronze_olist_order_payments_dataset,
        on="order_id"
    )
    
    return Output(
		value=fact_sales_data,
		metadata={
			"table": MetadataValue.text("fact_sales"),
			"records count": MetadataValue.int(len(fact_sales_data)),
		}
	)
