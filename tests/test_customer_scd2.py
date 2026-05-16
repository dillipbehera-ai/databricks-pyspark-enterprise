from datetime import date
from pyspark.sql import SparkSession
from src.customer_scd2 import scd_type2_transform


def test_scd_type2_changed_customer():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("scd2-test") \
        .getOrCreate()

    source_df = spark.createDataFrame(
        [(1, "Dillip", "Hyderabad", "d@gmail.com")],
        ["customer_id", "name", "city", "email"]
    )

    target_df = spark.createDataFrame(
        [(1, "Dillip", "Bangalore", "d@gmail.com", date(2026, 5, 1), None, True)],
        ["customer_id", "name", "city", "email", "start_date", "end_date", "is_current"]
    )

    result_df = scd_type2_transform(source_df, target_df)

    assert result_df.count() == 2
    assert result_df.filter("is_current = true").count() == 1
    assert result_df.filter("is_current = false").count() == 1
    assert result_df.filter("city = 'Hyderabad' and is_current = true").count() == 1

    spark.stop()