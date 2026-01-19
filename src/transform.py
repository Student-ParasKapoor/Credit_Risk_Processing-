from pyspark.sql.functions import sum as spark_sum, count, col, when
from ingest import ingest_credit_data

def compute_monthly_exposure(df):
    monthly_exposure_df = (
        df.groupBy("report_month")
          .agg(spark_sum("outstanding_balance").alias("total_exposure"))
          .orderBy("report_month")
    )
    return monthly_exposure_df

def compute_monthly_default_rate(df):
    default_rate_df = (
        df.groupBy("report_month")
          .agg(
              count("*").alias("total_loans"),
              spark_sum("default_flag").alias("defaulted_loans")
          )
          .withColumn(
              "default_rate",
              col("defaulted_loans")/col("total_loans")
          )
    )
    return default_rate_df

def compute_monthly_delinquency_rate(df):
    delinquency_df = (
        df.groupBy("report_month")
          .agg(
              count("*").alias("total_loans"),
              spark_sum(when(col("days_past_due") > 30, 1).otherwise(0)).alias("delinquent_loans")
          )
          .withColumn(
              "delinquency_rate",
              col("delinquent_loans") / col("total_loans")
          )
          .orderBy("report_month")
    )
    return delinquency_df

if __name__ == "__main__":
    df = ingest_credit_data("data/raw/credit_risk_data.csv")

    print("Monthly Exposure:")
    compute_monthly_exposure(df).show()

    print("Monthly Default Rate:")
    compute_monthly_default_rate(df).show()

    print("Monthly Delinquency Rate (DPD > 30):")
    compute_monthly_delinquency_rate(df).show()

    compute_monthly_exposure(df).write \
        .mode("overwrite") \
        .parquet("outputs/metrics/exposure")

    compute_monthly_default_rate(df).write \
        .mode("overwrite") \
        .parquet("outputs/metrics/default_rate")

    compute_monthly_delinquency_rate(df).write \
        .mode("overwrite") \
        .parquet("outputs/metrics/delinquency_rate")
