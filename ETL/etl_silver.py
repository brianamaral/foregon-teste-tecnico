from pandas import to_timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, to_timestamp

from pyspark.sql.types import StructType
import boto3
import datetime

spark = SparkSession.builder.getOrCreate()

spark.conf.set("spark.sql.session.timeZone", "UTC")

user_data_raw = spark.read.option("multiline", "true").json(
    "s3a://foregon-raw/dump_date=2021-01-18/*.json"
)

user_data_raw.write.mode("overwrite").parquet(
    f"s3a://foregon-silver/user_data/ingestion_date={datetime.date.today()}"
)
