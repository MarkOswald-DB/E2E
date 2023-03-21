# Databricks notebook source
import mlflow
import pyspark.sql.functions as f
from pyspark.sql.functions import col
from databricks import feature_store
from pyspark.sql import Window

# COMMAND ----------

dbutils.widgets.text('experiment_name', '/Users/riley.rustad@databricks.com/hls_readmissions_demo')
experiment_name = dbutils.widgets.get('experiment_name')

dbutils.widgets.text('model_path', 'model')
model_path = dbutils.widgets.get('model_path')

dbutils.widgets.text('model_name', 'riley_hls_e2e')
model_name = dbutils.widgets.get('model_name')

# COMMAND ----------

def fetch_webhook_data(): 
  try:
    registry_event = json.loads(dbutils.widgets.get('event_message'))
    model_name = registry_event['model_name']
    model_version = registry_event['version']
    if 'to_stage' in registry_event and registry_event['to_stage'] != 'Staging':
      dbutils.notebook.exit()
  except:
    #If it's not in a job but interactive demo, we get the last version from the registry
    model_name = 'demos_customer_churn'
    model_version = client.get_registered_model(model_name).latest_versions[0].version
  return(model_name, model_version)

# COMMAND ----------

client = mlflow.tracking.MlflowClient()
fs = feature_store.FeatureStoreClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch Current Staging Model

# COMMAND ----------

model_details = client.get_latest_versions(model_name, ['Staging'])#[0]
model_details

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Target Variable to Validate

# COMMAND ----------

max_enc_date = spark.table('riley_e2e.encounters').select(f.max(f.col('STOP'))).collect()[0][0]

windowSpec = Window.partitionBy("PATIENT").orderBy("START")

val_data = (
  spark.table('riley_e2e.encounters')
  .withColumn('30_DAY_READMISSION',f.when(col('START').cast('long') - f.lag(col('STOP')).over(windowSpec).cast('long') < 30*24*60*60, 1).otherwise(0))
  .filter(f.lit(max_enc_date).cast('long') - col('START').cast('long') > (30*24*60*60))
  .select('Id', 'PATIENT', 'START', 'STOP','30_DAY_READMISSION')
  .orderBy(['STOP'],ascending=False)
  .dropna()
  .limit(1000)
)

# COMMAND ----------

val_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use Feature Store to Score Batch
# MAGIC Notice, we didn't need to rewrite our data prep ETL for inference. It's a single line of code to score data with the proper key

# COMMAND ----------

preds = fs.score_batch(f"models:/{model_name}/{model_details.version}", val_data)
preds.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check for Model Fairness

# COMMAND ----------

(
  preds
  .groupby('GENDER_F')
  .agg(
    f.sum(
      f.when(f.col('30_DAY_READMISSION') == f.col('prediction'), 1).otherwise(0)
    )/f.sum(f.lit(1)).alias('accuracy')
  )
).display()

# COMMAND ----------

(
  preds
  .groupby('RACE_white')
  .agg(
    f.sum(
      f.when(f.col('30_DAY_READMISSION') == f.col('prediction'), 1).otherwise(0)
    )/f.sum(f.lit(1)).alias('accuracy')
  )
).display()

# COMMAND ----------

client.get_model_version(name=model_name, version=model_details.version)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate That Model Has Necessary Metadata

# COMMAND ----------

# TODO

# COMMAND ----------


