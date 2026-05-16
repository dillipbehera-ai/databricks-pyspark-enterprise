from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, sha2, current_date, lit


def add_hash(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "hash_value",
        sha2(concat_ws("||", col("name"), col("city"), col("email")), 256)
    )


def scd_type2_transform(source_df: DataFrame, target_df: DataFrame) -> DataFrame:
    source_df = add_hash(source_df)
    target_df = add_hash(target_df)

    current_target_df = target_df.filter(col("is_current") == True)

    changed_df = source_df.alias("s").join(
        current_target_df.alias("t"),
        col("s.customer_id") == col("t.customer_id"),
        "inner"
    ).filter(col("s.hash_value") != col("t.hash_value"))

    new_df = source_df.alias("s").join(
        current_target_df.alias("t"),
        col("s.customer_id") == col("t.customer_id"),
        "left_anti"
    )

    changed_ids_df = changed_df.select(col("s.customer_id").alias("customer_id"))

    unchanged_df = target_df.alias("t").join(
        changed_ids_df,
        "customer_id",
        "left_anti"
    ).select(
        "customer_id",
        "name",
        "city",
        "email",
        "start_date",
        "end_date",
        "is_current"
    )

    expired_df = changed_df.select(
        col("t.customer_id").alias("customer_id"),
        col("t.name").alias("name"),
        col("t.city").alias("city"),
        col("t.email").alias("email"),
        col("t.start_date").alias("start_date"),
        current_date().alias("end_date"),
        lit(False).alias("is_current")
    )

    inserted_changed_df = changed_df.select(
        col("s.customer_id").alias("customer_id"),
        col("s.name").alias("name"),
        col("s.city").alias("city"),
        col("s.email").alias("email"),
        current_date().alias("start_date"),
        lit(None).cast("date").alias("end_date"),
        lit(True).alias("is_current")
    )

    inserted_new_df = new_df.select(
        col("s.customer_id").alias("customer_id"),
        col("s.name").alias("name"),
        col("s.city").alias("city"),
        col("s.email").alias("email"),
        current_date().alias("start_date"),
        lit(None).cast("date").alias("end_date"),
        lit(True).alias("is_current")
    )

    return unchanged_df.unionByName(expired_df) \
        .unionByName(inserted_changed_df) \
        .unionByName(inserted_new_df)