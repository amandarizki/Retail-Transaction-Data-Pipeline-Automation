from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import regexp_replace, monotonically_increasing_id
import shutil
import os

def transform(df: DataFrame) -> DataFrame:
    """
    Transformation steps:
    1. Add a unique TransactionID column.
    2. Clean newline characters in the StoreLocation column.
    """
    df = df.withColumn("TransactionID", monotonically_increasing_id())
    df = df.withColumn("StoreLocation", regexp_replace("StoreLocation", r"\n", " "))
    return df

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Transform ETL") \
        .getOrCreate()

    # Load extracted data
    input_path = "opt/airflow/data/extracted.csv"  # Change this if your file is in a different location
    df_raw = spark.read.csv(input_path, header=True, inferSchema=True)

    # Apply transformations
    df_transformed = transform(df_raw)

    # Write the transformed DataFrame to a temporary folder
    temp_output_path = "opt/airflow/data/transformed_temp"
    df_transformed.coalesce(1).write \
        .option("header", True) \
        .mode("overwrite") \
        .csv(temp_output_path)

    # Rename the generated part file to transformed.csv
    final_output_path = "opt/airflow/data/transformed.csv"
    for file_name in os.listdir(temp_output_path):
        if file_name.endswith(".csv"):
            shutil.move(os.path.join(temp_output_path, file_name), final_output_path)
            break

    # Remove the temporary folder
    shutil.rmtree(temp_output_path)

    print(f"[Transform] Transformed data successfully saved to: {final_output_path}")
