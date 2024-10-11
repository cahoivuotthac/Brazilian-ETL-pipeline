import os
from contextlib import contextmanager
from datetime import datetime
from typing import Union

from dotenv import load_dotenv
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from minio import Minio
import sqlalchemy

load_dotenv()

@contextmanager
def connect_minio(config: dict):
    client = Minio(
        endpoint=os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("AWS_ACCESS_KEY_ID"),
        secret_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        secure=False
    )
    
    try:
        yield client
    except Exception:
        raise

class MinIOIOManager(IOManager):
   
    def __init__(self, config):
        self._config = config
        
    def _get_path(self, context: Union[InputContext, OutputContext]):
        
        """
        Original asset name: bronze_olist_order_items_dataset
        context.asset_key.path = ["bronze", "ecom", "bronze_olist_order_items_dataset"]
        After processing:
            layer = "bronze"
            schema = "ecom"
            table = "bronze_olist_order_items_dataset"
            key = "olist_order_items_dataset"
        """
        
        layer, schema, table = context.asset_key.path
        key = "/".join([layer, schema, table.replace(f"{layer}_", "")])
        tmp_file_path = "/tmp/file-{}-{}.parquet".format(
            datetime.today().strftime("%Y%m%d%H%M%S"), "-".join(context.asset_key.path)
        )
        return f"{key}.pq", tmp_file_path

    def handle_output(self, context: OutputContext, obj: pd.DataFrame): # save a pandas dataframe to MinIO
        key_name, tmp_file_path = self._get_path(context)
        
        try:
            # Convert DataFrame to parquet and save to tmp file
            obj.to_parquet(tmp_file_path)
            
            # Upload to MinIO
            with connect_minio(self._config) as client:
                client.fput_object(
                    bucket_name=os.getenv("DATALAKE_BUCKET"),
                    object_name=key_name,
                    file_path=tmp_file_path
                )
            
            # Clean up tmp file
            os.remove(tmp_file_path)
        except Exception:
            # Make sure to clean up the tmp file even if an error occurs
            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)
            raise

    def load_input(self, context: InputContext) -> pd.DataFrame: # read parquet file from MinIO and convert it into dataframe
        key_name, tmp_file_path = self._get_path(context)
        # key_name = "bronze/ecom/olist_order_items_dataset.pq"
        # tmp_file_path = "/tmp/file-20210901120000-bronze-ecom-bronze_olist_order_items_dataset.parquet"
        
        try:
            # Download file from MinIO
            with connect_minio(self._config) as client:
                client.fget_object(
                    bucket_name=os.getenv("DATALAKE_BUCKET"),
                    object_name=key_name,
                    file_path=tmp_file_path,
                    
                )
                
            context.log.info(f"Downloading from bucket: {os.getenv('DATALAKE_BUCKET')}, object: {key_name}")
            
            # Read the downloaded parquet file into a DataFrame
            df = pd.read_parquet(tmp_file_path)
            
            context.log.info("Convert parquet file into a DataFrame")
            context.log.info(df.info)
            
            # Clean up temporary file
            os.remove(tmp_file_path)
            
            return df
        except Exception:
            # Ensure temporary file is removed in case of an error
            if os.path.exists(tmp_file_path):
                os.remove(tmp_file_path)
            raise
    