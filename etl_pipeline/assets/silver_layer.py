# use this layer combining with tables in bronze layer to create dim_products @asset and fact_sales @asset MinIO -> MinIO
# MinIO (silver/ecom/<table_name>)

from io import StringIO
from dagster import Definitions, asset, Output
import pandas as pd
from minio import Minio

from week2_practice.etl_pipeline.assets.bronze_layer import MINIO_CONFIG
from week2_practice.etl_pipeline.resources.minio_io_manager import MinIOIOManager

# Constants
SLIVER_BUCKET = "silver/ecom"
DIM_PRODUCTS_OBJECT = "dim_products.csv"
FACT_SALES_OBJECT = "fact_sales.csv"

def fetch_data_csv(minio_client: Minio, key:str) -> pd.DataFrame:
	try:
		data = minio_client.get_object(key)
		return pd.read_csv(StringIO(data.read().decode('utf-8')))
	except Exception as e:
		raise RuntimeError(f"Failed to fetch data from MinIO: {e}")

@asset(
    io_manage_key="minio_io_manager",
	required_resource_keys={"minio_resource"},
	key_prefix=["silver", "ecom"],
	compute_kind="MinIO"
)
def silver_dim_products(context) -> Output[pd.DataFrame]:
	minio_client = context.resources.minio_resource
 
	#Fetch data from MinIO
	products_df = fetch_data_csv(minio_client, "bronze/ecom/olist_products_dataset.csv")
	category_df = fetch_data_csv(minio_client, "bronze/ecom/product_category_name_translation.csv")
 
	# SQL join 
	dim_products_df = products_df.merge(
		category_df,
		on="product_category_name",
	)[["product_id", "product_category_name_english"]]
 
	csv_data = dim_products_df.to_csv(index=False)
 
	# Upload to MinIO
	try:
		minio_client.put_object(
			SLIVER_BUCKET,
			DIM_PRODUCTS_OBJECT,
			data=StringIO(csv_data),
			length=len(csv_data),
			content_type="text/csv"
		)
	except Exception as e:
		raise RuntimeError(f"Failed to upload data to MinIO: {e}")

	return Output(
		dim_products_df,
		metadata={
			"table": "dim_products",
			"records count": len(dim_products_df)
		},
	)
 
@asset(
    io_manage_key="minio_io_manager",
	required_resource_keys={"minio_resource"},
	key_prefix=["silver", "ecom"],
	compute_kind="MinIO"
)
def silver_fact_sales(context) -> Output[pd.DataFrame]:
	minio_client = context.resources.minio_resource
	
	# Fetch data from MinIO
	orders_df = fetch_data_csv(minio_client, "bronze/ecom/olist_orders_dataset.csv")
	order_items_df = fetch_data_csv(minio_client, "bronze/ecom/olist_order_items_dataset.csv")
	payments_df = fetch_data_csv(minio_client, "bronze/ecom/olist_order_payments_dataset.csv")
 
	# SQL join
	fact_sales = orders_df.merge(
		order_items_df,
		on="order_id"
	).merge(
		payments_df,
		on="order_id"
	)[["order_id", "customer_id", "order_purchase_timestamp", "product_id", "payment_value", "order_status"]]
	
	csv_data = fact_sales.to_csv(index=False)
 
	# Upload to MinIO
	try:
		minio_client.put_object(
			SLIVER_BUCKET,
			FACT_SALES_OBJECT,
			data=StringIO(csv_data),
			length=len(csv_data),
			content_type="text/csv"
		)
	except Exception as e:
		raise RuntimeError(f"Failed to upload data to MinIO: {e}")

	return Output(
		fact_sales,
		metadata={
			"table": "fact_sales",
			"records count": len(fact_sales),
		},
	)
 
defs = Definitions(
	assets=[silver_dim_products, silver_fact_sales],
	resources={
    	"minio_io_manager": MinIOIOManager(MINIO_CONFIG)
    }
)

