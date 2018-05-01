
# coding: utf-8
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from os import environ, path

# warehouse_location points to the default location for managed databases and tables
warehouse_location = path.abspath('spark-warehouse')

environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.databricks:spark-xml_2.11:0.4.1 pyspark-shell' 
spark = SparkSession \
    .builder \
    .appName("Python Spark wordcount") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport().getOrCreate()

schema = StructType([
    StructField('w', ArrayType(StringType()))
])

# read xml files as df
df_xml = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag", "s") \
    .option("rootTag", "document") \
    .load("./subtitles/lt_all/*.xml", schema=schema) # spark does not support recursive load

df_xml.write.mode("overwrite").saveAsTable("nl")

spark.sparkContext.stop()
