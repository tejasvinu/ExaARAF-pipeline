from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, max, min, from_utc_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import time
import logging
import os

# Set timezone to IST
os.environ['TZ'] = 'Asia/Kolkata'
time.tzset()

# Configure logging
logging.basicConfig(level=logging.ERROR, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                   datefmt='%Y-%m-%d %Z')
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create a local Spark session for stability"""
    return SparkSession.builder \
        .appName("MetricsPipeline") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,com.datastax.spark:spark-cassandra-connector_2.12:3.3.0") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.streaming.backpressure.enabled", "true") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.scheduler.mode", "FAIR") \
        .config("spark.sql.session.timeZone", "Asia/Kolkata") \
        .getOrCreate()

def process_metrics(spark, topic_name, metric_type):
    """Process metrics from Kafka to Cassandra"""
    try:
        # Define schema for Prometheus metrics
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("value", DoubleType(), True),
            StructField("labels", StringType(), True)
        ])
        
        # Read from Kafka
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", topic_name) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parse JSON data and convert timestamp to IST
        parsed_df = df \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*") \
            .withColumn("timestamp", from_utc_timestamp(col("timestamp"), "Asia/Kolkata"))
        
        # Write to Cassandra - without window aggregation
        query = parsed_df \
            .writeStream \
            .outputMode("append") \
            .foreachBatch(lambda batch_df, batch_id: 
                batch_df.write \
                    .format("org.apache.spark.sql.cassandra") \
                    .options(table=f"{metric_type}_metrics", keyspace="metrics") \
                    .mode("append") \
                    .save()
            ) \
            .option("checkpointLocation", f"/tmp/checkpoints/{topic_name}") \
            .start()
        
        logger.info(f"Started streaming query for {topic_name}")
        return query
    
    except Exception as e:
        logger.error(f"Error processing {topic_name}: {str(e)}")
        raise

if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    logger.info("Starting metrics processing pipeline")
    
    topics = [
        ("ipmi_metrics", "ipmi"),
        ("node_metrics", "node"),
        ("dcgm_metrics", "dcgm"),
        ("slurm_metrics", "slurm")
    ]
    
    queries = []
    
    # Start all streaming queries
    for topic_name, metric_type in topics:
        try:
            query = process_metrics(spark, topic_name, metric_type)
            queries.append(query)
        except Exception as e:
            logger.error(f"Failed to start query for {topic_name}: {str(e)}")
    
    # Monitor queries and keep app running
    try:
        while any(query.isActive for query in queries if query is not None):
            time.sleep(30)
            active_count = sum(1 for q in queries if q is not None and q.isActive)
            logger.info(f"Active queries: {active_count}/{len(queries)}")
        
        logger.warning("All queries have terminated")
    except KeyboardInterrupt:
        logger.info("Stopping application due to user interrupt")
        for query in queries:
            if query is not None and query.isActive:
                query.stop()
    finally:
        spark.stop()
        logger.info("Spark session stopped")
