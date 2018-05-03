# coding: utf-8

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count
from pyspark.ml.feature import NGram, StopWordsRemover
from os import environ, path

parser = argparse.ArgumentParser()
parser.add_argument('--language', help='give language to parse', required=True)
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
    .appName("Python Spark ngrams") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport().getOrCreate()

# print spark webinterface url
print("Web URL:", spark.sparkContext.uiWebUrl)

# set subtitle language to make ngram of
language = args.language

# init stopword remover
stopwords_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=[".", ",", "'"]) 
# init ngram maker
ngram = NGram(n=2, inputCol="filtered_words", outputCol="ngrams")  

# check if table exists for language, if not: quit
if language not in [table.name for  table in spark.catalog.listTables()]:
    print("Hive tables for language", language, "was not found")
    exit(-1)

#read table
df_subtitles = spark.sql("SELECT * FROM " + language).select(col('w').alias('words'))

# remove stopwords
df_words_clean = stopwords_remover.transform(df_subtitles.dropna())
df_words_clean = df_words_clean.drop("words").dropna()

# make ngrams with n=2 (words)
df_ngrams = ngram.transform(df_words_clean) 
df_ngrams = df_ngrams \
    .drop("filtered_words") \
    .dropna() \
    .withColumn("ngrams", explode(col("ngrams"))) \
    .groupBy("ngrams").agg(count(col("ngrams"))) \
    .select(col("ngrams"),col("count(ngrams)").alias("frequency")) \
    .filter("frequency > 1") \
    .sort(col("frequency").desc())

# save ngrams and stop spark
print("saving dataframe...")
df_ngrams.write.mode("overwrite").saveAsTable("ng_"+language)
print("saved", language)

spark.sparkContext.stop()