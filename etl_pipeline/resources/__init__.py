import os

from dotenv import load_dotenv
from resources.psql_io_manager import PostgreSQLIOManager
from resources.minio_io_manager import MinIOIOManager
from resources.mysql_io_manager import MySQLIOManager

load_dotenv()

MYSQL_CONFIG = MySQLIOManager(
    {
		"host": os.getenv("MYSQL_HOST"),
		"port": os.getenv("MYSQL_PORT"),
		"database": os.getenv("MYSQL_DATABASE"),
		"user": os.getenv("MYSQL_USER"),
		"password": os.getenv("MYSQL_PASSWORD")
	}
)

MINIO_CONFIG = MinIOIOManager(
    {
		"endpoint_url": os.getenv("MINIO_ENDPOINT"),
		"bucket": os.getenv("DATALAKE_BUCKET"),
		"aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
		"aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY")
	}
)

PSQL_CONFIG = PostgreSQLIOManager(
	{
		"host": os.getenv("POSTGRES_HOST"),
		"port": os.getenv("POSTGRES_PORT"),
		"database": os.getenv("POSTGRES_DB"),
		"user": os.getenv("POSTGRES_USER"),
		"password": os.getenv("POSTGRES_PASSWORD"),
	}
)