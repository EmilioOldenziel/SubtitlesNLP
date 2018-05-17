# coding: utf-8

import argparse
from pyspark.sql.functions import col, explode, count, lower
from spark import Spark

parser = argparse.ArgumentParser()
parser.add_argument('--language', help='give language to parse', required=True)
args = parser.parse_args()

# create spark session
sp = Spark()
spark_session = sp.make_spark_session()

language = args.language

df_subtitles = sp.load_table(spark_session, language)

df_wc = df_subtitles \
    .withColumn("word", explode(col("w"))) \
    .drop("w") \
    .withColumn('word', lower(col('word'))) \
    .groupBy("word").agg(count(col("word"))) \
    .select(col("word"),col("count(word)").alias("frequency")) \

sp.save_table(df_wc, "wc_"+language)

sp.stop(spark_session)