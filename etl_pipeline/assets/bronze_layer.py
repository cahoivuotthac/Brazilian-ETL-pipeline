import os
from dotenv import load_dotenv
import pandas as pd
from dagster import AssetExecutionContext, Definitions, MetadataValue, asset, Output

from resources import MINIO_CONFIG, MYSQL_CONFIG

load_dotenv()

GROUP_NAME="bronze_layer"

@asset(
    required_resource_keys={"mysql_io_manager"}, #input: load data from MySQL
    io_manager_key="minio_io_manager", #output: save data to MinIO
    key_prefix=["bronze", "ecom"],
    compute_kind="MySQL",
    group_name=GROUP_NAME
)
def bronze_olist_order_items_dataset(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM olist_order_items_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    
    if pd_data is None:
        raise ValueError("No data fetched from MySQL database for table olist_order_items_dataset")
    return Output( 
        pd_data, 
        metadata={
            "table": MetadataValue.text("olist_order_items_dataset"),
            "records count": MetadataValue.int(len(pd_data)),
        },
    )

@asset(
    required_resource_keys={"mysql_io_manager"},
    io_manager_key="minio_io_manager",
    key_prefix=["bronze", "ecom"],
    compute_kind="MySQL",
    group_name=GROUP_NAME
)
def bronze_olist_order_payments_dataset(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM olist_order_payments_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    
    if pd_data is None:
        raise ValueError("No data fetched from MySQL database for table olist_order_payments_dataset")
    return Output(
        pd_data, 
        metadata={
            "table": MetadataValue.text("olist_order_payments_dataset"),
            "records count": MetadataValue.int(len(pd_data)),
        },
    )

@asset(
    required_resource_keys={"mysql_io_manager"},
    io_manager_key="minio_io_manager",
    key_prefix=["bronze", "ecom"],
    compute_kind="MySQL",
    group_name=GROUP_NAME
)
def bronze_olist_orders_dataset(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM olist_orders_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    
    if pd_data is None:
        raise ValueError("No data fetched from MySQL database for table olist_orders_dataset")
    return Output(
        pd_data, 
        metadata={
            "table": MetadataValue.text("olist_orders_dataset"),
            "records count": MetadataValue.int(len(pd_data)),
        },
    )

@asset(
    required_resource_keys={"mysql_io_manager"},
    io_manager_key="minio_io_manager",
    key_prefix=["bronze", "ecom"],
    compute_kind="MySQL",
    group_name=GROUP_NAME
)
def bronze_olist_products_dataset(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM olist_products_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    
    if pd_data is None:
        raise ValueError("No data fetched from MySQL database for table olist_products_dataset")
    return Output(
        pd_data, 
        metadata={
            "table": MetadataValue.text("olist_products_dataset"),
            "records count": MetadataValue.int(len(pd_data)),
        },
    )

@asset(
    required_resource_keys={"mysql_io_manager"},
    io_manager_key="minio_io_manager",
    key_prefix=["bronze", "ecom"],
    compute_kind="MySQL",
    group_name=GROUP_NAME
)
def bronze_product_category_name_translation(context: AssetExecutionContext) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM product_category_name_translation"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    
    if pd_data is None:
        raise ValueError("No data fetched from MySQL database for table product_category_name_translation")
    return Output(
        pd_data, 
        metadata={
            "table": MetadataValue.text("product_category_name_translation"),
            "records count": MetadataValue.int(len(pd_data)),
        },
    )

defs = Definitions(
	assets=[
        bronze_olist_order_items_dataset,
        bronze_product_category_name_translation,
        bronze_olist_order_items_dataset,
        bronze_olist_order_payments_dataset,
        bronze_olist_products_dataset
    ],
	resources={
		"mysql_io_manager": MYSQL_CONFIG,
		"minio_io_manager": MINIO_CONFIG,
	}
)