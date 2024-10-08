# use this layer combining with tables in silver_layer to create sales_values_by_category
# @asset (gold/ecom/<table_name>)

from io import StringIO
from dagster import Definitions, asset, Output
import pandas as pd
from minio import Minio
import datetime as dt

from week2_practice.etl_pipeline.assets.bronze_layer import MINIO_CONFIG
from week2_practice.etl_pipeline.resources.minio_io_manager import MinIOIOManager

# Constants
GOLD_BUCKET = "gold/ecom"
SALES_VALUES_BY_CATEGORY_OBJECT = "sales_values_by_category.csv"
FACT_SALES_OBJECT = "fact_sales.csv"
DIM_PRODUCTS_OBJECT = "dim_products.csv"

def fetch_data_csv(minio_client: Minio, key:str) -> pd.DataFrame:
	try:
		data = minio_client.get_object(key)
		return pd.read_csv(StringIO(data.read().decode('utf-8')))
	except Exception as e:
		raise RuntimeError(f"Failed to fetch data from MinIO: {e}")

@asset(
	io_manager_key="minio_io_manager",
	required_resource_keys={"minio_resource"},
	key_prefix=["gold", "ecom"],
	compute_kind="MinIO"
)
def gold_daily_sales_products(context) -> Output[pd.DataFrame]:
	minio_client = context.resources.minio_resource
 
	#Fetch data from MinIO
	fact_sales_df = fetch_data_csv(minio_client, FACT_SALES_OBJECT)
	dim_products_df = fetch_data_csv(minio_client, DIM_PRODUCTS_OBJECT)
	
	fact_sales_df["order_purchase_timestamp"] = fact_sales_df["order_purchase_timestamp"].dt.date
	# SQL query
	daily_sales_products = (
		fact_sales_df[fact_sales_df["order_status"] == "delivered"]
		.groupby([fact_sales_df["order_purchase_timestamp"].dt.date, "product_id"])
		.agg(
			sales=pd.NamedAgg(column="payment_value", aggfunc=lambda x: round(x.astype(float).sum(), 2)),
			bills=pd.NamedAgg(column="order_id", aggfunc="nunique"),
		)
		.reset_index()
		.rename(columns={"order_purchase_timestamp": "daily"})
	)
	
	daily_sales_categories = (
		daily_sales_products
			.merge(dim_products_df, on="product_id")
			.agg(
				monthly=lambda x: x["daily"].dt.to_period("y-MM"),
				values_per_bills=lambda x: x["sales"] / x["bills"]
			)
			.rename(columns={"product_category_name_english": "category"})
	)
 
	sales_values_by_category = (
		daily_sales_categories
		.groupby(["monthly", "category"])
		.agg(
			total_sales=("sales", "sum"),
			total_bills=("bills", "sum"),
			values_per_bills=lambda x: sum(x["sales"]) * 1.0 / sum(x["bills"])
		)
		.reset_index()
	)
 
	# Convert to CSV
	csv_data = sales_values_by_category.to_csv(index=False)
 
	# Upload to MinIO
	try: 
		minio_client.put_object(
			GOLD_BUCKET,
			SALES_VALUES_BY_CATEGORY_OBJECT,
			data=StringIO(csv_data),
			length=len(csv_data),
			content_type="text/csv"
		)
	except Exception as e:
		raise RuntimeError(f"Failed to upload data to MinIO: {e}")

	return Output(
		sales_values_by_category,
		metadata={
			"table": "sales_values_by_category",
			"records count": len(sales_values_by_category)
		}
	)
 
defs = Definitions(
	assets=gold_daily_sales_products,
	resources={
		"minio_io_manager": MinIOIOManager(MINIO_CONFIG)
	}
)