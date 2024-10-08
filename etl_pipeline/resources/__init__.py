import os

from dotenv import load_dotenv

load_dotenv()

MYSQL_CONFIG = {
	"host": os.getenv("MYSQL_HOST"),
	"port": os.getenv("MYSQL_PORT"),
	"database": os.getenv("MYSQL_DATABASE"),
	"user": os.getenv("MYSQL_USER"),
	"password": os.getenv("MYSQL_PASSWORD")
}

MINIO_CONFIG = {
	"endpoint_url": os.getenv("MINIO_ENDPOINT"),
	"bucket": os.getenv("DATALAKE_BUCKET"),
	"aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
	"aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY")
}