from dagster import AssetExecutionContext, AssetIn, AssetOut, Definitions, MetadataValue, Output, multi_asset
import pandas as pd

GROUP_NAME="gold_layer"

@multi_asset(
	ins={
		"dim_products": AssetIn(
			key_prefix=["silver", "ecom"]
		),
		"fact_sales": AssetIn(
			key_prefix=["silver", "ecom"]
		)
	},
	outs={
		"gold_sales_values_by_category": AssetOut(
			io_manager_key="minio_io_manager",
			key_prefix=["gold", "ecom"],
			metadata={
				"primary_keys": "product_category_name_english",
				"columns": [
					"product_category_name_english",
					"sales_value"
				],
			}
		)
	},
	compute_kind="MinIO",
	group_name=GROUP_NAME
)
def gold_sales_values_by_category(
    context: AssetExecutionContext,
	dim_products: pd.DataFrame,
	fact_sales: pd.DataFrame
) -> Output[pd.DataFrame]:
    
    fact_sales['order_purchase_timestamp'] = pd.to_datetime(fact_sales['order_purchase_timestamp'])
    
    fact_sales['order_purchase_date'] = fact_sales["order_purchase_timestamp"].dt.date
    
    # 1: Create daily_sales_products 
    daily_sales_products = (
        fact_sales[fact_sales['order_status'] == 'delivered']
        .groupby(['order_purchase_date', 'product_id'])
        .agg(
            sales=pd.NamedAgg(column="payment_value", aggfunc=lambda x: round(x.astype(float).sum(), 2)),
            bills=(pd.NamedAgg(column="order_id", aggfunc="nunique"))
		)
        .reset_index()
        .rename(columns={'order_purchase_date': 'daily'})
    )
    
    # 2: Create daily_sales_categories 
    daily_sales_categories = (
        daily_sales_products
        .merge(dim_products[['product_id', 'product_category_name']], on='product_id')
        .assign(
            monthly=lambda df: pd.to_datetime(df['daily']).dt.to_period('M').astype(str),
            values_per_bills=lambda df: df['sales'] / df['bills']
        )
    )
    
    # 3: Aggregate 
    gold_sales_values_by_category = (
        daily_sales_categories
        .groupby(['monthly', 'product_category_name'])
        .agg(
            total_sales=('sales', 'sum'),
            total_bills=('bills', 'sum'),
            values_per_bills=('values_per_bills', 'mean')
        )
        .reset_index()
        .rename(columns={'product_category_name': 'category'})
    )
	
    return Output(
		gold_sales_values_by_category,
		metadata={
			"schema": MetadataValue.text("public"),
			"table": MetadataValue.text("gold_sales_values_by_category"),
			"records count": MetadataValue.int(len(gold_sales_values_by_category)),
		}
	)
    