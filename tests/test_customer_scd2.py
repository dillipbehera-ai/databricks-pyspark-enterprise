from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from src.customer_scd2 import scd_type2_transform


def test_scd_type2_changed_customer():

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("scd2-test") \
        .getOrCreate()

    source_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("email", StringType(), True)
    ])

    target_schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("email", StringType(), True),
        StructField("start_date", DateType(), True),
        StructField("end_date", DateType(), True),
        StructField("is_current", BooleanType(), True)
    ])

    source_data = [
        (1, "Dillip", "Hyderabad", "d@gmail.com")
    ]

    target_data = [
        (1, "Dillip", "Bangalore", "d@gmail.com", date(2026, 5, 1), None, True)
    ]

    source_df = spark.createDataFrame(source_data, source_schema)

    target_df = spark.createDataFrame(target_data, target_schema)

    result_df = scd_type2_transform(source_df, target_df)

    result_df.show()

    assert result_df.count() == 2

    assert result_df.filter("is_current = true").count() == 1

    assert result_df.filter("is_current = false").count() == 1

    assert result_df.filter(
        "city = 'Hyderabad' and is_current = true"
    ).count() == 1

    spark.stop()