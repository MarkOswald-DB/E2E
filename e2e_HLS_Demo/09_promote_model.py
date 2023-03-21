# Databricks notebook source
import mlflow
import json
from mlflow.utils.rest_utils import http_request

# COMMAND ----------

dbutils.widgets.text('experiment_name', '/Users/riley.rustad@databricks.com/hls_readmissions_demo')
experiment_name = dbutils.widgets.get('experiment_name')

dbutils.widgets.text('model_path', 'model')
model_path = dbutils.widgets.get('model_path')

dbutils.widgets.text('model_name', 'riley_hls_e2e')
model_name = dbutils.widgets.get('model_name')

# COMMAND ----------

client = mlflow.tracking.MlflowClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Look Up Current Staging Model

# COMMAND ----------

model_details = client.get_latest_versions(model_name, ['Staging'])[0]
model_details

# COMMAND ----------

# MAGIC %md
# MAGIC ### Programmatically Promote Model to Production

# COMMAND ----------

model_details = client.transition_model_version_stage(
    name=model_details.name,
    version=model_details.version,
    stage="Production"
)
model_details

# COMMAND ----------


