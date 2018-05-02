# coding: utf-8

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count
from pyspark.ml.feature import NGram, StopWordsRemover, Tokenizer
from os import environ, path

parser = argparse.ArgumentParser()
parser.add_argument('--l', help='give language', required=True)
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

print("web URL", spark.sparkContext.uiWebUrl)
language = args.l

#stopwords_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=[".", ","]) # init stopword remover
ngram = NGram(n=2, inputCol="words", outputCol="ngrams")  # init ngram maker

df_subtitles = spark.sql("SELECT * FROM " + language).select(col('w').alias('words'))

# df_words = stopwords_remover \
#     .transform(df_subtitles) \
#     .drop("words") \

df_words = ngram.transform(df_subtitles) # make ngrams with n=2 (words)
df_words = df_words.drop("words").withColumn("ngrams", explode(col("ngrams")))

print(df_words.count())

print("saving dataframe...")
df_words.write.mode("overwrite").saveAsTable("ng_"+language)
print("saved", language)

spark.sparkContext.stop()