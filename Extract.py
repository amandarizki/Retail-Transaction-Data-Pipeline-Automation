from pyspark.sql import SparkSession

def load_data(file_path):
    spark = SparkSession.builder \
        .appName("ETL_Extract_PySpark") \
        .getOrCreate()

    df = spark.read.csv(
        file_path,
        header=True,
        inferSchema=True,
        sep=",",
        multiLine=True,
        escape='"'  
    )
    return df

if __name__ == "__main__":
    input_path = "opt/airflow/data/data_raw.csv"
    output_path = "opt/airflow/data/extracted.csv"  

    df = load_data(input_path)

    # Show 5 rows
    df.show(5)

    # Save as CSV
    df.write \
        .option("header", True) \
        .mode("overwrite") \
        .csv(output_path)
