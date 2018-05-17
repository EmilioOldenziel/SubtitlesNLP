from pyspark.sql import SparkSession
from os import environ, path

class Spark():

    def __init__(self, **kwargs):
        self.warehouse_location = path.abspath('spark-warehouse')
        self.set_submit_args()

    def set_submit_args(self, memory="15g"):
        # load spark-xml and set driver memory (150GB)
        submit_args = [
            '--packages com.databricks:spark-xml_2.11:0.4.1',
            '--driver-memory '+ memory,
            'pyspark-shell'
        ]
        environ['PYSPARK_SUBMIT_ARGS'] =  " ".join(submit_args)

    def make_spark_session(self):
        return SparkSession \
            .builder \
            .appName("Python Spark ngrams") \
            .config("spark.sql.warehouse.dir", self.warehouse_location) \
            .enableHiveSupport().getOrCreate()

    def print_spark_url(self, spark):
        # print spark webinterface url
        print("Web URL:", spark.sparkContext.uiWebUrl)


    def stop(self, spark):
        spark.sparkContext.stop()

    def table_exists(self, spark, table_name):
        if table_name not in [t.name for t in spark.catalog.listTables()]:
            print("Hive table", table_name, "was not found")
            return 0
        else:
            return 1

    def load_table(self, spark, table_name):
        return spark.sql("SELECT * FROM " + table_name)

    def save_table(self, df, name):
        # save ngrams and stop spark
        print("saving dataframe...")
        df.write.mode("overwrite").saveAsTable(name)
        print("saved", name)

    def save_as_csv(self, df, table_name):
        df.repartition(1).write.csv(table_name + '.csv')