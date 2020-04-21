import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


schema = StructType([
    StructField("crime_id", StringType(), False),
    StructField("original_crime_type_name", StringType(), False),
    StructField("report_date", StringType(), False),
    StructField("call_date", StringType(), False),
    StructField("offense_date", StringType(), False),
    StructField("call_time", StringType(), False),
    StructField("call_date_time", StringType(), False),
    StructField("disposition", StringType(), False),
    StructField("address", StringType(), False),
    StructField("city", StringType(), True),
    StructField("state", StringType(), False),
    StructField("agency_id", StringType(), False),
    StructField("address_type", StringType(), False),
    StructField("common_location", StringType(), True)
])

def run_spark_job(spark):

    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "udacity.com.km.sf.crime") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")
    
    
    
    service_table = kafka_df\
        .select(psf.from_json(psf.col("value").cast("string"), schema).alias("DF"))\
        .select("DF.*")
    
    
    
    # select original_crime_type_name and disposition
    distinct_table = service_table \
            .selectExpr('original_crime_type_name', 'disposition', 'to_timestamp(call_date_time) as call_date_time') \
            .withWatermark("call_date_time", "60 minutes") 
    
    # count the number of original crime type
    agg_df = distinct_table\
        .withWatermark("call_date_time", "60 minutes") \
        .groupBy(psf.window(distinct_table.call_date_time, "60 minutes", "10 minutes"), distinct_table.original_crime_type_name) \
        .count().selectExpr('original_crime_type_name as octn', 'count', 'window.start as start_window', 'window.end as end_window')
    
    query_df = distinct_table.join(agg_df, \
            (distinct_table.original_crime_type_name == agg_df.octn) \
            & (distinct_table.call_date_time >= agg_df.start_window)  \
            & (distinct_table.call_date_time < agg_df.end_window), \
            "inner")
                                                               
    # Submit a screen shot of a batch ingestion of the aggregation
    query = query_df \
            .select('original_crime_type_name', 'disposition', 'call_date_time', 'count')\
            .writeStream \
            .format("console") \
            .trigger(processingTime="3 seconds")\
            .start()
    
    

    
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.option("multiline", "true").json(radio_code_json_filepath)
    
    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")
    
    j_query =  query_df \
        .join(radio_code_df, "disposition", "leftouter")\
        .select('call_date_time', 'original_crime_type_name', 'count', 'disposition', 'description') \
        .writeStream \
        .format("console") \
        .start()
    
    query.awaitTermination()
    j_query.awaitTermination()
    
    
if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port", 3000)\
        .getOrCreate()
    
    logger.info("Spark started")
    
    #spark.sparkContext.setLogLevel('WARN')
    
    run_spark_job(spark)

    spark.stop()
