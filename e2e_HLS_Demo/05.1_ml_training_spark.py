# Databricks notebook source
from databricks.feature_store import FeatureLookup
from databricks import feature_store
import mlflow
import pyspark.sql.functions as f
from pyspark.sql.functions import col

# COMMAND ----------

dbutils.widgets.text('dbName', 'riley_e2e')
dbName = dbutils.widgets.get('dbName')

dbutils.widgets.text('experiment_name', '/Users/riley.rustad@databricks.com/hls_readmissions_demo')
experiment_name = dbutils.widgets.get('experiment_name')

# COMMAND ----------

max_enc_date = spark.table('riley_e2e.encounters').select(f.max(f.col('STOP'))).collect()[0][0]
max_enc_date

# COMMAND ----------

from pyspark.sql import Window

windowSpec = Window.partitionBy("PATIENT").orderBy("START")

data = (
  spark.table('riley_e2e.encounters')
  .withColumn('30_DAY_READMISSION',f.when(col('START').cast('long') - f.lag(col('STOP')).over(windowSpec).cast('long') < 30*24*60*60, 1).otherwise(0))
  .filter(f.lit(max_enc_date).cast('long') - col('START').cast('long') > (30*24*60*60))
  .select('Id', 'PATIENT', 'START', 'STOP', '30_DAY_READMISSION')
  .orderBy(['PATIENT','START'])
)
  
data.display()

# COMMAND ----------

train, val, test = data.randomSplit([0.8, 0.1, 0.1])

# COMMAND ----------

["Id","PATIENT"]
 
patient_features_table = f'{dbName}.pat_features'
encounter_features_table = f'{dbName}.enc_features'
age_at_enc_features_table = f'{dbName}.age_at_enc_features'
 
patient_feature_lookups = [
   FeatureLookup( 
     table_name = patient_features_table,
     feature_names = [
      'MARITAL_M',
      'MARITAL_S',
      'RACE_asian',
      'RACE_black',
      'RACE_hawaiian',
      'RACE_other',
      'RACE_white',
      'ETHNICITY_hispanic',
      'ETHNICITY_nonhispanic',
      'GENDER_F',
      'GENDER_M',
      'INCOME'],
     lookup_key = ["PATIENT"],
   ),
]
 
encounter_feature_lookups = [
   FeatureLookup( 
     table_name = encounter_features_table,
     feature_names = ['BASE_ENCOUNTER_COST', 'TOTAL_CLAIM_COST', 'PAYER_COVERAGE', 'enc_length', 'ENCOUNTERCLASS_ambulatory', 'ENCOUNTERCLASS_emergency', 'ENCOUNTERCLASS_hospice', 'ENCOUNTERCLASS_inpatient', 'ENCOUNTERCLASS_outpatient', 'ENCOUNTERCLASS_wellness',],
     lookup_key = ["Id"],
   ),
]

age_at_enc_feature_lookups = [
   FeatureLookup( 
     table_name = age_at_enc_features_table,
     feature_names = ['age_at_encounter'],
     lookup_key = ["Id"],
   ),
]

# COMMAND ----------

fs = feature_store.FeatureStoreClient()

training_set = fs.create_training_set(
  train,
  feature_lookups = patient_feature_lookups + encounter_feature_lookups + age_at_enc_feature_lookups,
  label = "30_DAY_READMISSION",
  exclude_columns = ["Id", "PATIENT", 'START', 'STOP']
).load_df()

val_set = fs.create_training_set(
  val,
  feature_lookups = patient_feature_lookups + encounter_feature_lookups + age_at_enc_feature_lookups,
  label = "30_DAY_READMISSION",
  exclude_columns = ["Id", "PATIENT", 'START', 'STOP']
).load_df()

test_set = fs.create_training_set(
  test,
  feature_lookups = patient_feature_lookups + encounter_feature_lookups + age_at_enc_feature_lookups,
  label = "30_DAY_READMISSION",
  exclude_columns = ["Id", "PATIENT", 'START', 'STOP']
).load_df()

# COMMAND ----------

mlflow.set_experiment(experiment_name)
target_col = "30_DAY_READMISSION"

# COMMAND ----------

from mlflow.tracking import MlflowClient

# COMMAND ----------

from sklearn.ensemble import RandomForestClassifier

# COMMAND ----------

import mlflow
import sklearn
from hyperopt import STATUS_OK, Trials, fmin, hp, tpe, SparkTrials
import mlflow.sklearn
from sklearn.metrics import roc_auc_score

def score(params):
  with mlflow.start_run() as mlflow_run:
    for key, value in params.items():
      mlflow.log_param(key, value)
    
    model = RandomForestClassifier(
      n_estimators = int(params['n_estimators']),
      max_depth = int(params['max_depth']),
      min_samples_split = int(params['min_samples_split'])
    )

    model.fit(train.drop(target_col,axis=1), train[target_col])

    preds = model.predict(val.drop(target_col,axis=1))
    score = roc_auc_score(val[target_col], preds)
    
    train_preds = model.predict(train.drop(target_col,axis=1))
    train_score = roc_auc_score(train[target_col], train_preds)
    
    mlflow.log_metric('val_auc', score)
    mlflow.log_metric('train_auc', score)
    mlflow.log_metric('diff', train_score - score)
    
    mlflow.sklearn.log_model(model, 'model')
  
    loss = 1 - score
  return {'loss': loss, 'status': STATUS_OK}

def optimize(
             #trials, 
             random_state=42):
    """
    This is the optimization function that given a space (space here) of 
    hyperparameters and a scoring function (score here), finds the best hyperparameters.
    """
    space = {
        'n_estimators': hp.quniform('n_estimators', 100, 1000, 1),
        'max_depth':  hp.choice('max_depth', range(1, 14)),
        'min_samples_split': hp.quniform('min_samples_split', 2, 6, 1),
        'random_state': random_state
    }
#     spark_trials = SparkTrials()
    # Use the fmin function from Hyperopt to find the best hyperparameters
    best = fmin(score, space, algo=tpe.suggest, 
#                 trials=spark_trials, 
                max_evals=50)
    return best

# COMMAND ----------

best_hyperparams = optimize(
                            #trials
                            )

# COMMAND ----------

from shap import KernelExplainer, summary_plot

try:
    # Sample background data for SHAP Explainer. Increase the sample size to reduce variance.
    train_sample = X_train.sample(n=min(100, len(X_train.index)))

    # Sample a single example from the validation set to explain. Increase the sample size and rerun for more thorough results.
    example = X_val.sample(n=1)

    # Use Kernel SHAP to explain feature importance on the example from the validation set.
    predict = lambda x: model.predict(pd.DataFrame(x, columns=X_train.columns))
    explainer = KernelExplainer(predict, train_sample, link="identity")
    shap_values = explainer.shap_values(example, l1_reg=False)
    summary_plot(shap_values, example)
except Exception as e:
    print(f"An unexpected error occurred while plotting feature importance using SHAP: {e}")

# COMMAND ----------

experiment_name = df = spark.read.format("mlflow-experiment").load("3270527066281272")

# COMMAND ----------

expId = mlflow.get_experiment_by_name(experiment_name).experiment_id
df = spark.read.format("mlflow-experiment").load(expId)
best_run_id = (
  df.orderBy('metrics.val_auc',ascending=False)
  .select('run_id')
  .limit(1)
  .collect()[0][0]
)
best_run_id

# COMMAND ----------


