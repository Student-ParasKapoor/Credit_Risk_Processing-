from spark_session import get_spark_session

def ingest_credit_data(path):
    spark = get_spark_session()

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(path)
    
    return df

if __name__ == "__main__":
    df = ingest_credit_data("data/raw/credit_risk_data.csv")
    df.show(5)
    df.printSchema()