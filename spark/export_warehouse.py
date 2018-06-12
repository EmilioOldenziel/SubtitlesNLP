 
#check if table actually exists
if not sp.table_exists(spark_session, table_name)
    exit(-1)

# load table and export to csv
df = sp.load_table(spark_session, table_name)
sp.save_as_csv(df, table_name)

# stop jvm spark session
sp.stop(spark_session)