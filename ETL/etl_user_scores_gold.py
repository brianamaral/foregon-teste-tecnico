from itertools import count
from pandas import to_timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, to_timestamp, when

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

user_data_products_unwrapped = user_data_products.select(
    flatten_struct(user_data_products.schema)
)

user_data_products_unwrapped = treat_column_names(user_data_products_unwrapped)

user_data_products_unwrapped = treat_date_columns(
    dataframe=user_data_products_unwrapped, date_format="yyyy-MM-dd'T'HH:mm:ss.SSSZ"
)

user_score= user_data_products_unwrapped.select(col("score_score")).withColumn(
    "score_group", when(col("score_score") <= 200, "ATE 200")\
                   .when((col('score_score') >=201) & (col('score_score') <= 400),'DE 201 a 400')\
                   .when((col('score_score') >= 401) & (col('score_score') <=600),'DE 401 A 600')\
                   .when((col('score_score') >= 601) & (col('score_score') <= 800),'DE 601 A 800')\
                   .otherwise('ACIMA DE 801')        
)

user_score_grouped = user_score.groupBy('score_group').count()

user_score_grouped.write.mode("overwrite").parquet(
    f"s3a://foregon-gold/user_data/user_data_score_grouped/ingestion_date={datetime.date.today()}"
)

