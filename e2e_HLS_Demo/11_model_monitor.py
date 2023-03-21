# Databricks notebook source
import mlflow
import pyspark.sql.functions as f
from pyspark.sql.functions import col
from databricks import feature_store

# COMMAND ----------

dbutils.widgets.text('experiment_name', '/Users/riley.rustad@databricks.com/hls_readmissions_demo')
experiment_name = dbutils.widgets.get('experiment_name')

dbutils.widgets.text('model_path', 'model')
model_path = dbutils.widgets.get('model_path')

dbutils.widgets.text('model_name', 'riley_hls_e2e')
model_name = dbutils.widgets.get('model_name')

dbutils.widgets.text('demo_path','dbfs:/home/riley.rustad@databricks.com/hlse2e')
demo_path=dbutils.widgets.get('demo_path')

output_path = demo_path+'/output'

dbutils.widgets.text('dbName', 'riley_e2e')
dbName = dbutils.widgets.get('dbName')

# COMMAND ----------

from pyspark.sql import Window

windowSpec = Window.partitionBy("PATIENT").orderBy("START")

max_enc_date = spark.table('riley_e2e.encounters').select(f.max(f.col('STOP'))).collect()[0][0]

data = (
  spark.table('riley_e2e.encounters')
  .withColumn('30_DAY_READMISSION',f.when(col('START').cast('long') - f.lag(col('STOP')).over(windowSpec).cast('long') < 30*24*60*60, 1).otherwise(0))
  .filter(f.lit(max_enc_date).cast('long') - col('START').cast('long') > (30*24*60*60))
  .select('Id', 'PATIENT', 'START', 'STOP', '30_DAY_READMISSION')
  .orderBy(['PATIENT','START'])
)
  
data.display()

# COMMAND ----------

preds = spark.table(f'{dbName}.readmission_predictions')

comp_data = (
  data.join(preds, preds.Id == data.Id, 'inner')
  .withColumn('correct', f.when(col('30_DAY_READMISSION') == col('prediction'), 1).otherwise(0))
  .withColumn('month', f.date_trunc('month',col('START')))
  .groupby('month')
  .agg(f.mean('correct').alias('accuracy'))
  .orderBy('month')
)
comp_data.display()

# COMMAND ----------

(
  spark.table(f'{dbName}.encounters')
  .withColumn('month', f.date_trunc('month',col('START')))
  .groupby('month')
  .count()
).display()

# COMMAND ----------


