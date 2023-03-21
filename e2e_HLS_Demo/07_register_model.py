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
# MAGIC ### Look Up Best Run within Experiment
# MAGIC I can do this programmatically or through the UI

# COMMAND ----------

expId = mlflow.get_experiment_by_name(experiment_name).experiment_id

df = spark.read.format("mlflow-experiment").load(expId)
best_run_id = (
  df.orderBy('metrics.val_auc',ascending=False)
  .select('run_id')
  .limit(1)
  .collect()[0][0]
)
# or
best_run_id = mlflow.search_runs(experiment_ids=[expId], order_by=["metrics.val_auc DESC"], max_results=1, filter_string="status = 'FINISHED'").iloc[0]['run_id']

best_run_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register The Best Model

# COMMAND ----------

model_details = mlflow.register_model(f"runs:/{best_run_id}/{model_path}", model_name)

# COMMAND ----------

model_details

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update Model With Descriptions Metatdata

# COMMAND ----------

model_version_details = client.get_model_version(name=model_name, version=model_details.version)

#The main model description, typically done once.
client.update_registered_model(
  name=model_details.name,
  description="This model predicts whether a patient will Readmit.  It is used to update the Readmissions Dashboard in DB SQL."
)

#Gives more details on this specific model version
client.update_model_version(
  name=model_details.name,
  version=model_details.version,
  description="This model version was built using XGBoost. Eating too much cake is the sin of gluttony. However, eating too much pie is okay because the sin of pie is always zero."
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Programmatically Transition Model to Production

# COMMAND ----------

host_creds = client._tracking_client.store.get_host_creds()
host = host_creds.host
token = host_creds.token

def mlflow_call_endpoint(endpoint, method, body='{}'):
  if method == 'GET':
      response = http_request(
          host_creds=host_creds, endpoint="/api/2.0/mlflow/{}".format(endpoint), method=method, params=json.loads(body))
  else:
      response = http_request(
          host_creds=host_creds, endpoint="/api/2.0/mlflow/{}".format(endpoint), method=method, json=json.loads(body))
  return response.json()

def request_transition(model_name, version, stage):
  
  staging_request = {'name': model_name,
                     'version': version,
                     'stage': stage,
                     'archive_existing_versions': 'true'}
  response = mlflow_call_endpoint('transition-requests/create', 'POST', json.dumps(staging_request))
  return(response)

# COMMAND ----------

request_transition(model_name = model_name, version = model_details.version, stage = "Staging")

# COMMAND ----------


