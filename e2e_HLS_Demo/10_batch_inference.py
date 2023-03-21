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

dbutils.widgets.text('demo_path','/FileStore/hls/synthea/data')
demo_path=dbutils.widgets.get('demo_path')

output_path = demo_path+'/output'

dbutils.widgets.text('dbName', 'riley_e2e')
dbName = dbutils.widgets.get('dbName')

# COMMAND ----------

dbutils.fs.ls(demo_path)

# COMMAND ----------

client = mlflow.tracking.MlflowClient()
fs = feature_store.FeatureStoreClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Look Up Production Model

# COMMAND ----------

model_details = client.get_latest_versions(model_name, ['Production'])[0]
model_details

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pull Inference Data

# COMMAND ----------

df = spark.readStream.format('delta').table('riley_e2e.encounters')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use Feature Store To Score Batch
# MAGIC You can stream in new inference data, score it, and write it out to another delta table in a single command.

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

preds = (
  fs.score_batch(f"models:/{model_name}/Production", df)
  .select('Id', 'prediction')
  .writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", f'{output_path}/checkpoint')
  .trigger(once=True)
  .toTable(f"{dbName}.readmission_predictions", path=f'{output_path}/readmissions_predictions')
  .awaitTermination()
)


# COMMAND ----------

display(spark.table(f"{dbName}.readmission_predictions"))

# COMMAND ----------

# spark.sql(f'drop table {dbName}.readmission_predictions')
# dbutils.fs.rm(f'{output_path}/readmissions_predictions', True)
# dbutils.fs.rm(f'{output_path}/checkpoint', True)

# COMMAND ----------


