import os
from dotenv import load_dotenv
import pandas as pd
from dagster import AssetExecutionContext, MetadataValue, asset, Output

from dagster import Definitions
from resources import MINIO_CONFIG, MYSQL_CONFIG, PSQL_CONFIG
load_dotenv()

@asset(
	required_resource_keys={"mysql_io_manager"},
	io_manager_key="minio_io_manager",
	key_prefix=["bronze", "ecom"],
	compute_kind="MySQL"
)
def bronze_olist_orders_dataset(context: AssetExecutionContext) -> Output[pd.DataFrame]:
	sql_stm = "SELECT * FROM olist_orders_dataset"
	pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
	if pd_data is None:
		raise ValueError("No data fetched from MySQL database")
	return Output(
		pd_data, 
		metadata={
			"table": MetadataValue.text("olist_orders_dataset"),
			"records count": MetadataValue.int(len(pd_data)),
		},
	)

defs = Definitions(
    assets=[bronze_olist_orders_dataset],
    resources={
        "mysql_io_manager": MYSQL_CONFIG,
        "minio_io_manager": MINIO_CONFIG,
        "psql_io_manager": PSQL_CONFIG
    }
)
