# Databricks notebook source
from databricks.feature_store import FeatureLookup
from databricks import feature_store
import mlflow
import pyspark.sql.functions as f
from pyspark.sql.functions import col
from pyspark.sql import Window
from mlflow.tracking import MlflowClient
from sklearn.ensemble import RandomForestClassifier

# COMMAND ----------

dbutils.widgets.text('dbName', 'riley_e2e')
dbName = dbutils.widgets.get('dbName')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define our Taget Variable `30_DAY_READMISSION`

# COMMAND ----------

max_enc_date = spark.table('riley_e2e.encounters').select(f.max(f.col('STOP'))).collect()[0][0]
max_enc_date

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

# MAGIC %md
# MAGIC ### Define What Features To Look Up

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

# MAGIC %md
# MAGIC ### Use Feature Store to Create Dataset Based on Lookups

# COMMAND ----------

fs = feature_store.FeatureStoreClient()
training_set = fs.create_training_set(
  data,
  feature_lookups = patient_feature_lookups + encounter_feature_lookups + age_at_enc_feature_lookups,
  label = "30_DAY_READMISSION",
#   exclude_columns = ["Id", "PATIENT", 'START', 'STOP']
)

training_df = training_set.load_df()

# COMMAND ----------

train, val, test = training_df.randomSplit([0.8, 0.1, 0.1])

# Since we're training on sklearn we'll convert data to pandas dataframes
train = train.toPandas()
val = val.toPandas()
test = test.toPandas()

# COMMAND ----------

train#[train.isna()]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define the MLFlow Experiement

# COMMAND ----------

mlflow.set_experiment('/Users/riley.rustad@databricks.com/hls_readmissions_demo')
target_col = "30_DAY_READMISSION"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Iterate Through Hyperparameters
# MAGIC Logging all ML Model parameters, metrics, and artifacts

# COMMAND ----------

import mlflow
import sklearn
from hyperopt import STATUS_OK, Trials, fmin, hp, tpe, SparkTrials
import mlflow.sklearn
from sklearn.metrics import roc_auc_score

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
    mlflow.log_metric('train_auc', train_score)
    mlflow.log_metric('diff', train_score - score)
    
#     mlflow.sklearn.log_model(model, 'model')
    fs.log_model(
      model,
      artifact_path="model",
      flavor=mlflow.sklearn,
      training_set=training_set,
#       registered_model_name="taxi_example_fare_packaged"
    )
  
    loss = 1 - score
  return {'loss': loss, 'status': STATUS_OK}


# COMMAND ----------

best_hyperparams = optimize(
                            #trials
                            )
