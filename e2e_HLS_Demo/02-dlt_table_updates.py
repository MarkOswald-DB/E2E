# Databricks notebook source
import dlt

# COMMAND ----------

demo_path=start_date = spark.conf.get("demo_path")
landed_path = demo_path+'/landed'
tables = spark.conf.get("bronze_tables").split(',')

# COMMAND ----------

def update_dlt_table(table_name):
  @dlt.table(name=table_name)
  def dlt_table():
    df = spark.read.parquet(landed_path+f'/{table_name}')
    return (
      spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .schema(df.schema)
        .load(landed_path+f'/{table_name}')
    )
    

# COMMAND ----------

for table_name in tables:
  update_dlt_table(table_name)
