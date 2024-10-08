import os
from dotenv import load_dotenv
import pandas as pd
from dagster import OutputContext, asset, Output, Definitions, AssetIn, multi_asset, AssetOut
from resources.minio_io_manager import MinIOIOManager
from resources.mysql_io_manager import MySQLIOManager
from resources.psql_io_manager import PostgreSQLIOManager
import psycopg2

@asset(
    io_manager_key="minio_io_manager",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "ecom"],
    compute_kind="MySQL" 
)
#! extract data from MySQL database
def bronze_olist_orders_dataset(context) -> Output[pd.DataFrame]:  # Removed the annotation
    sql_stm = "SELECT * FROM olist_orders_dataset"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(
        pd_data,
        metadata={
            "table": "olist_orders_dataset",
            "records count": len(pd_data),
        },
    )

@multi_asset(
    ins={
            "bronze_olist_orders_dataset": AssetIn(
            key_prefix=["bronze", "ecom"]
        )
    },
    outs={
        "olist_orders_dataset": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            metadata={
                "primary_keys": [
                    "order_id",
                    "customer_id",
                ],
                "columns": [
                    "order_id",
                    "customer_id",
                    "order_status",
                    "order_purchase_timestamp",
                    "order_approved_at",
                    "order_delivered_carrier_date",
                    "order_delivered_customer_date",
                    "order_estimated_delivery_date",
                ],
            },
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

defs = Definitions(
    assets=[bronze_olist_orders_dataset, olist_orders_dataset],
    resources={
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
    },
)