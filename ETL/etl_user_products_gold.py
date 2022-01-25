from pandas import to_timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, to_timestamp

from pyspark.sql.types import StructType
import boto3
import datetime

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")


def flatten_struct(schema, prefix="") -> list:
    result = []
    for elem in schema:
        if isinstance(elem.dataType, StructType):
            result += flatten_struct(elem.dataType, prefix + elem.name + ".")
        else:
            result.append(col(prefix + elem.name).alias(prefix + elem.name))
    return result


def treat_column_names(dataframe):
    columns = dataframe.schema.names
    for column in columns:
        dataframe = dataframe.withColumnRenamed(column, column.replace(".", "_"))

    return dataframe


def treat_date_columns(dataframe, date_format: str):
    columns = dataframe.schema.names

    for column in columns:

        if column.endswith("_$date"):

            dataframe = dataframe.withColumn(column, to_timestamp(column, date_format))

    return dataframe


user_data_products = spark.read.option("multiline", "true").parquet(
     f"s3a://foregon-silver/user_data/ingestion_date={datetime.date.today()}"
)

user_data_products = user_data_products.withColumn(
    "alreadyHaveProducts", explode("alreadyHaveProducts")
)

user_data_products_unwrapped = user_data_products.select(
    flatten_struct(user_data_products.schema)
)

user_data_products_unwrapped = treat_column_names(user_data_products_unwrapped)

user_data_products_unwrapped = treat_date_columns(
    dataframe=user_data_products_unwrapped, date_format="yyyy-MM-dd'T'HH:mm:ss.SSSZ"
)

user_data_products_unwrapped.write.mode("overwrite").parquet(
    f"s3a://foregon-gold/user_data/user_data_products/ingestion_date={datetime.date.today()}"
)
