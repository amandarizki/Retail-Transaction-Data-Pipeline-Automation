from pyspark.sql import SparkSession, DataFrame
from pymongo import MongoClient
import os

def save_to_mongodb(df: DataFrame, db_name: str, collection_name: str):
    """
    Save a Spark DataFrame to MongoDB.
    The DataFrame will be converted to a Pandas DataFrame before inserting.

    Parameters:
        df (DataFrame): Spark DataFrame to save.
        db_name (str): Name of the target MongoDB database.
        collection_name (str): Name of the target MongoDB collection.

    Environment Variables:
        MONGO_URI: MongoDB connection string.
    """
    # Get MongoDB URI from environment variable
    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri:
        raise ValueError("Environment variable MONGO_URI is not set.")

    pandas_df = df.toPandas()
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]
    collection.insert_many(pandas_df.to_dict(orient='records'))
    print(f"[Load] Data successfully saved to MongoDB in database: {db_name}, collection: {collection_name}")

if __name__ == "__main__":
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("Load ETL to MongoDB") \
        .getOrCreate()

    # Load transformed data from CSV
    input_path = "opt/airflow/data/transformed.csv"
    df_transformed = spark.read.csv(input_path, header=True, inferSchema=True)

    # Save the DataFrame to MongoDB
    save_to_mongodb(df_transformed, "P2M3_Database", "TransactionData")
