# coding: utf-8

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from os import path, environ

parser = argparse.ArgumentParser()
parser.add_argument('--table', help='give table to export', required=True)
args = parser.parse_args()

# warehouse_location points to the default location for managed databases and tables
warehouse_location = path.abspath('spark-warehouse')

# set driver memory
submit_args = [
    '--driver-memory 15g',
    'pyspark-shell'
]
environ['PYSPARK_SUBMIT_ARGS'] =  " ".join(submit_args)

spark = SparkSession \
    .builder \
    .appName("Python Spark wordcount") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

table = args.table

if table not in [t.name for t in spark.catalog.listTables()]:
    print("Hive tables", table, "was not found")
    exit(-1)

df = spark.sql("SELECT * FROM " + table)

print("exporting dataframe...")
df.repartition(1).write.csv(table + '.csv')
print("exported", table)

spark.sparkContext.stop()
