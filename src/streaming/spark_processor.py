import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def start_streaming_processor():
    # Schema configuration matching api_gateway
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("merchant", StringType(), True),
        StructField("device", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

    # Initialiser la session Spark configurée avec Apache Iceberg et MinIO
    spark = SparkSession.builder \
        .appName("EDP-Medallion-Bronze-Streaming") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg_catalog.type", "hadoop") \
        .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3a://iceberg-warehouse/bronze") \
        .config("spark.sql.catalog.iceberg_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://localhost:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "supersecretpassword") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    print("Session PySpark configurée pour Iceberg + S3.")

    # Lecture du flux de données depuis Kafka (Topic 'transactions')
    kafka_servers = os.getenv("KAFKA_SERVERS", "localhost:9092")
    
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", "transactions") \
        .option("startingOffsets", "latest") \
        .load()

    # Décodage du message JSON en colonnes structurées
    df_parsed = df_raw \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), schema).alias("data")) \
        .select("data.*")

    # Ajout d'une colonne de partition temporelle
    df_enriched = df_parsed.withColumn("date_partition", col("timestamp").substr(1, 10))

    # Écriture en continu vers la table Apache Iceberg (Couche Bronze)
    print("Démarrage du streaming vers la table Iceberg...")
    
    query = df_enriched.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .option("path", "iceberg_catalog.bronze.transactions_raw") \
        .option("checkpointLocation", "s3a://iceberg-warehouse/checkpoints/transactions_raw") \
        .start()

    query.awaitTermination()

if __name__ == '__main__':
    start_streaming_processor()
