import pyspark
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql import functions as F
from pyspark.sql import types as T
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json

dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

spark_hostname = os.getenv("SPARK_MASTER_HOST_NAME")
spark_port = os.getenv("SPARK_MASTER_PORT")
kafka_host = os.getenv("KAFKA_HOST")
kafka_topic = os.getenv("KAFKA_TOPIC_NAME")

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE")
CASSANDRA_USERNAME = os.getenv("CASSANDRA_USER")
CASSANDRA_PASSWORD = os.getenv("CASSANDRA_PASSWORD")

CASSANDRA_TABLE = 'WeatherData'

spark_host = f"spark://{spark_hostname}:{spark_port}"

# Spark session configuration
spark = SparkSession.builder \
    .appName("WeatherStreaming") \
    .config("spark.cassandra.connection.host", "dataeng-cassandra") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .config("spark.cassandra.connection.protocolVersion", "4") \
    .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.3.2,"
                                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .getOrCreate()

kafka_schema = StructType([
    StructField("ID", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Weather_Condition", StringType(), True),
    StructField("Temperature_Celsius", DoubleType(), True),
    StructField("Humidity_Percentage", DoubleType(), True),
    StructField("Wind_Speed", DoubleType(), True),
    StructField("Timestamp", StringType(), True),
])

# Read from Kafka stream
stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{kafka_host}:9092")
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "latest")
    .load()
)

# Parse JSON and convert Timestamp field
parsed_df = stream_df.select(from_json(col("value").cast("string"), kafka_schema).alias("parsed_value")) \
    .select("parsed_value.*")

# Convert Timestamp field to TimestampType
parsed_df = parsed_df.withColumn("Timestamp", col("Timestamp").cast(TimestampType()))

# Function to create Cassandra connection
def create_cassandra_connection():
    try:
        cluster = Cluster(
            [CASSANDRA_HOST],
            port=int(CASSANDRA_PORT),
            auth_provider=PlainTextAuthProvider(
                username=CASSANDRA_USERNAME, password=CASSANDRA_PASSWORD
            ),
            protocol_version=4
        )
        session = cluster.connect()
        return session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None

# Function to create keyspace in Cassandra
def create_keyspace(session):
    try:
        session.execute(f"CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};")
        logging.info("Keyspace created successfully!")
    except Exception as e:
        logging.error(f"Could not create keyspace due to {e}")

# Function to create table in Cassandra
def create_table(session):
    try:
        session.execute(f"CREATE TABLE IF NOT EXISTS {CASSANDRA_KEYSPACE}.{CASSANDRA_TABLE} (ID UUID PRIMARY KEY, City TEXT, Weather_Condition TEXT, Temperature_Celsius DOUBLE, Humidity_Percentage DOUBLE, Wind_Speed DOUBLE, Timestamp TIMESTAMP);")
        logging.info("Table created successfully!")
    except Exception as e:
        logging.error(f"Could not create table due to {e}")

# Function to insert data into Cassandra
def insert_data(session, batch_df):
    try:
        batch_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("spark.cassandra.connection.host", CASSANDRA_HOST) \
            .option("spark.cassandra.connection.port", CASSANDRA_PORT) \
            .option("keyspace", CASSANDRA_KEYSPACE) \
            .option("table", CASSANDRA_TABLE) \
            .mode("append") \
            .save()
        logging.info("Data inserted into Cassandra successfully!")
    except Exception as e:
        logging.error(f"Could not insert data into Cassandra due to {e}")

# Process each batch and write to Cassandra
def process_stream(batch_df, batch_id):
    print(f"Processing batch {batch_id}")
    batch_df.show(truncate=False)

    # Create Cassandra connection
    cassandra_session = create_cassandra_connection()
    if cassandra_session is not None:
        # Create keyspace and table if not exists
        create_keyspace(cassandra_session)
        create_table(cassandra_session)

        # Insert data into Cassandra
        insert_data(cassandra_session, batch_df)

# Trigger Spark Streaming query
query = (
    parsed_df.writeStream
    .outputMode("append")
    .foreachBatch(process_stream)
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()