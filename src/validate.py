from pyspark.sql.functions import (
    col,
    count,
    min as spark_min,
    max as spark_max,
    sequence,
    explode,
    trunc,
    expr
)

from ingest import ingest_credit_data


def find_duplicate_records(df):
    duplicate_keys = (
        df.groupBy("loan_id", "report_month")
          .agg(count("*").alias("record_count"))
          .filter(col("record_count") > 1)
    )

    duplicate_records_df = (
        df.join(
            duplicate_keys,
            on=["loan_id", "report_month"],
            how="inner"
        )
    )

    return duplicate_records_df



def find_null_records(df):
    critical_columns = [
        "loan_id",
        "customer_id",
        "report_month",
        "outstanding_balance",
        "default_flag"
    ]

    null_condition = None
    for c in critical_columns:
        condition = col(c).isNull()
        null_condition = condition if null_condition is None else null_condition | condition

    null_records_df = df.filter(null_condition)

    return null_records_df



def find_missing_months(df):

    df_month = df.withColumn("month", trunc(col("report_month"), "month"))


    loan_ranges = (
        df_month.groupBy("loan_id")
        .agg(
            spark_min("month").alias("start_month"),
            spark_max("month").alias("end_month")
        )
    )


    expected_months = (
        loan_ranges
        .withColumn(
            "expected_month",
            explode(
                sequence(
                    col("start_month"),
                    col("end_month"),
                    expr("interval 1 month")
                )
            )
        )
        .select("loan_id", "expected_month")
    )


    actual_months = (
        df_month
        .select(
            col("loan_id"),
            col("month").alias("actual_month")
        )
        .distinct()
    )

    e = expected_months.alias("e")
    a = actual_months.alias("a")

    missing_months_df = (
        e.join(
            a,
            (col("e.loan_id") == col("a.loan_id")) &
            (col("e.expected_month") == col("a.actual_month")),
            how="left"
        )
        .filter(col("a.actual_month").isNull())
        .select(
            col("e.loan_id").alias("loan_id"),
            col("e.expected_month")
        )
    )

    return missing_months_df



if __name__ == "__main__":
    df = ingest_credit_data("data/raw/credit_risk_data.csv")

    print("Duplicate Records:")
    find_duplicate_records(df).show()

    print("Null Records:")
    find_null_records(df).show()

    print("Missing Month Records:")
    find_missing_months(df).show()


    find_duplicate_records(df).write \
        .mode("overwrite") \
        .parquet("outputs/exceptions/duplicates")

    find_null_records(df).write \
        .mode("overwrite") \
        .parquet("outputs/exceptions/nulls")

    find_missing_months(df).write \
        .mode("overwrite") \
        .parquet("outputs/exceptions/missing_months")
