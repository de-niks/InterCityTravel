from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
import os
from config import configuration


def main():
    #os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk:1.11.469,org.apache.kafka:kafka-clients:3.2.0'
    os.environ['PYSPARK_SUBMIT_ARGS'] = ('--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,'
                                         'org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk:1.11.469')
    packages = [
        f'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0',
        'org.apache.hadoop:hadoop-aws:3.4.0',
        'com.amazonaws:aws-java-sdk:1.11.469']

    spark = SparkSession \
        .builder.appName("SmartCityStreaming") \
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.s3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
        .config("spark.hadoop.fs.s3a.access.key",configuration.get("AWS_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get("AWS_SECRET_KEY")) \
        .config("spark.jars.packages", ",".join(packages)) \
        .config("spark.dynamicAllocation.enabled", "false") \
        .getOrCreate()

    # Adjust the log level to Warn
    # spark.sparkContext.setLogLevel('ERROR')

    # vehicle Schema

    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
    ])

    # GPS Location schema
    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("vehicleType", StringType(), True),
        StructField("direction", StringType(), True),

    ])

    # traffic Schema
    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("cameraId", StringType(), True),
        StructField("snapshot", StringType(), True),

    ])

    # weatherScehma
    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", DoubleType(), True),
    ])

    ## Emergancy Schema
    emergencyInformationSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("status", StringType(), True),

    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'broker:29092')
                .option('subscribe', topic)
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr('CAST(value AS STRING)')
                .select(from_json(col('value'), schema).alias('data'))
                .select('data.*')
                .withWatermark('timestamp', '2 minutes')
                )

    def stream_writer(input: DataFrame, checkpointfolder, output):
        return (input.writeStream
                .format('parquet')
                .outputMode('append')
                .format("console")
                .start())

    vehicleDF = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    trafficDF = read_kafka_topic('traffic_data', trafficSchema).alias('traffic')
    weatherDF = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_data', emergencyInformationSchema).alias('emergency')

    query1 = stream_writer(vehicleDF, "s3a//da-youtube-analytics-useast1/spark-streaming/checkpoints/vehicle_data",
                           "s3a//da-youtube-analytics-useast1/spark-streaming/data/vehicle_data")

    query2 = stream_writer(gpsDF, "s3a//da-youtube-analytics-useast1/spark-streaming/checkpoints/gps_data",
                           "s3a//da-youtube-analytics-useast1/spark-streaming/data/gps_data")
    query3 = stream_writer(trafficDF, "s3a//da-youtube-analytics-useast1/spark-streaming/checkpoints/traffic_data",
                           "s3a//da-youtube-analytics-useast1/spark-streaming/data/traffic_data")

    query4 = stream_writer(weatherDF, "s3a//da-youtube-analytics-useast1/spark-streaming/checkpoints/weather_data",
                           "s3a//da-youtube-analytics-useast1/spark-streaming/data/weather_data")

    query5 = stream_writer(emergencyDF, "s3a//da-youtube-analytics-useast1/spark-streaming/checkpoints/emergency_data",
                           "s3a//da-youtube-analytics-useast1/spark-streaming/data/emergency_data")

    query5.awaitTermination()


if __name__ == "__main__":
    main()
