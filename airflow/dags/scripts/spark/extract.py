import findspark
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

findspark.init()
def init_spark_and_schema():
    # Define the schema and spark session for the data extraction
    spark = SparkSession.builder \
        .appName("Fire Incident") \
        .master("local[1]") \
        .config("spark.driver.host", "host.docker.internal") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.dynamicAllocation.enabled", "false") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "120s") \
        .config("spark.executor.instances", "1") \
        .config("spark.cores.max", "1") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.memory", "512m") \
        .config("spark.driver.memory", "512m") \
        .config('spark.executor.extraClassPath',
                './jars/postgresql-42.7.3.jar') \
        .getOrCreate()

    schema = StructType([
        StructField("Incident Number", IntegerType(), True),
        StructField("Exposure Number", IntegerType(), True),
        StructField("ID", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("Incident Date", DateType(), True),
        StructField("Call Number", IntegerType(), True),
        StructField("Alarm DtTm", TimestampType(), True),
        StructField("Arrival DtTm", TimestampType(), True),
        StructField("Close DtTm", TimestampType(), True),
        StructField("City", StringType(), True),
        StructField("zipcode", StringType(), True),
        StructField("Battalion", StringType(), True),
        StructField("Station Area", StringType(), True),
        StructField("Box", StringType(), True),
        StructField("Suppression Units", IntegerType(), True),
        StructField("Suppression Personnel", IntegerType(), True),
        StructField("EMS Units", IntegerType(), True),
        StructField("EMS Personnel", IntegerType(), True),
        StructField("Other Units", IntegerType(), True),
        StructField("Other Personnel", IntegerType(), True),
        StructField("First Unit On Scene", StringType(), True),
        StructField("Estimated Property Loss", IntegerType(), True),
        StructField("Estimated Contents Loss", IntegerType(), True),
        StructField("Fire Fatalities", IntegerType(), True),
        StructField("Fire Injuries", IntegerType(), True),
        StructField("Civilian Fatalities", IntegerType(), True),
        StructField("Civilian Injuries", IntegerType(), True),
        StructField("Number of Alarms", IntegerType(), True),
        StructField("Primary Situation", StringType(), True),
        StructField("Mutual Aid", StringType(), True),
        StructField("Action Taken Primary", StringType(), True),
        StructField("Action Taken Secondary", StringType(), True),
        StructField("Action Taken Other", StringType(), True),
        StructField("Detector Alerted Occupants", StringType(), True),
        StructField("Property Use", StringType(), True),
        StructField("Area of Fire Origin", StringType(), True),
        StructField("Ignition Cause", StringType(), True),
        StructField("Ignition Factor Primary", StringType(), True),
        StructField("Ignition Factor Secondary", StringType(), True),
        StructField("Heat Source", StringType(), True),
        StructField("Item First Ignited", StringType(), True),
        StructField("Human Factors Associated with Ignition", StringType(), True),
        StructField("Structure Type", StringType(), True),
        StructField("Structure Status", StringType(), True),
        StructField("Floor of Fire Origin", IntegerType(), True),
        StructField("Fire Spread", StringType(), True),
        StructField("No Flame Spread", StringType(), True),
        StructField("Number of floors with minimum damage", IntegerType(), True),
        StructField("Number of floors with significant damage", IntegerType(), True),
        StructField("Number of floors with heavy damage", IntegerType(), True),
        StructField("Number of floors with extreme damage", IntegerType(), True),
        StructField("Detectors Present", StringType(), True),
        StructField("Detector Type", StringType(), True),
        StructField("Detector Operation", StringType(), True),
        StructField("Detector Effectiveness", StringType(), True),
        StructField("Detector Failure Reason", StringType(), True),
        StructField("Automatic Extinguishing System Present", StringType(), True),
        StructField("Automatic Extinguishing Sytem Type", StringType(), True),
        StructField("Automatic Extinguishing Sytem Perfomance", StringType(), True),
        StructField("Automatic Extinguishing Sytem Failure Reason", StringType(), True),
        StructField("Number of Sprinkler Heads Operating", IntegerType(), True),
        StructField("Supervisor District", IntegerType(), True),
        StructField("neighborhood_district", StringType(), True),
        StructField("point", StringType(), True),
        StructField("data_as_of", TimestampType(), True),
        StructField("data_loaded_at", TimestampType(), True)
    ])
    return spark, schema

def extract_data(spark, schema):
    df = spark.read \
        .schema(schema) \
        .format('csv') \
        .option('header', 'True') \
        .option('dateFormat', 'yyyy/MM/dd') \
        .option('timestampFormat', 'yyyy/MM/dd hh:mm:ss a') \
        .load('/opt/spark/data/raw/Fire_Incidents_20240516.csv')
    return df
