# Databricks notebook source
# MAGIC %md
# MAGIC # 30 Day Readmissions Feature Engineering
# MAGIC Our first step is to analyze the data and build the features we'll use to train our model. Let's see how this can be done.
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/product_demos/mlops-end2end-flow-1.png" width="1200">
# MAGIC 
# MAGIC <!-- do not remove -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&cid=555&aip=1&t=event&ec=field_demos&ea=display&dp=%2F42_field_demos%2Ffeatures%2Fmlops%2F02_feature_prep&dt=MLOPS">
# MAGIC <!-- [metadata={"description":"MLOps end2end workflow: Feature engineering",
# MAGIC  "authors":["quentin.ambard@databricks.com"],
# MAGIC  "db_resources":{},
# MAGIC   "search_tags":{"vertical": "retail", "step": "Data Engineering", "components": ["feature store"]},
# MAGIC                  "canonicalUrl": {"AWS": "", "Azure": "", "GCP": ""}}] -->

# COMMAND ----------

from pyspark.sql.functions import col
import pyspark.sql.functions as f
import pyspark.pandas as ps
from databricks import feature_store

dbutils.widgets.text('dbName', 'riley_e2e')
dbName = dbutils.widgets.get('dbName')

# COMMAND ----------

# Instantiate the Feature Store Client
fs = feature_store.FeatureStoreClient()

# COMMAND ----------

display(spark.table('riley_e2e.patients'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Data Logic for Feature Tables
# MAGIC In this case we're doing some one hot encoding leveraging `get_dummies` from the Pandas API on Spark

# COMMAND ----------

# Define Patient Features logic
def compute_pat_features(data):
  data = data.to_pandas_on_spark()
  data = ps.get_dummies(data, 
                        columns=['MARITAL', 'RACE', 'ETHNICITY', 'GENDER'],dtype = 'int64').to_spark()
  return data
  return (
    data
    .select(
      'Id',
      'BIRTHDATE',
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
      'GENDER_M'
      'INCOME',
    )
  )

# COMMAND ----------

pat_features_df = compute_pat_features(spark.table('riley_e2e.patients'))
pat_features_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Register Feature Store Table
# MAGIC Define the Feature store schema and and description, then actually populate the data into the table

# COMMAND ----------

# Create Dataframe using 
pat_features_df = compute_pat_features(spark.table('riley_e2e.patients'))


pat_feature_table = fs.create_table(
  name=f'{dbName}.pat_features',
  primary_keys=['Id'],
  df=pat_features_df,
  description='Base Features from the Patient Table'
# TODO add path arg to create_table()
#   , path='...'
)

fs.write_table(df=pat_features_df, name=f'{dbName}.pat_features', mode='merge')

# COMMAND ----------

def compute_enc_features(data):
  data = (
    data
    .withColumn('enc_length', f.unix_timestamp(col('stop'))- f.unix_timestamp(col('start')))
  )
  data = data.to_pandas_on_spark()
#   return data
  data = ps.get_dummies(data, 
                        columns=['ENCOUNTERCLASS'],dtype = 'int64').to_spark()
  
  return (
    data
    .select(
      col('Id').alias('ENCOUNTER_ID'),
      col('PATIENT').alias('PATIENT_ID'),
      'BASE_ENCOUNTER_COST',
      'TOTAL_CLAIM_COST',
      'PAYER_COVERAGE',
      'enc_length',
      'ENCOUNTERCLASS_ambulatory',
      'ENCOUNTERCLASS_emergency',
      'ENCOUNTERCLASS_hospice',
      'ENCOUNTERCLASS_inpatient',
      'ENCOUNTERCLASS_outpatient',
      'ENCOUNTERCLASS_wellness',
    )
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define and Populate Additional Feature Store Tables

# COMMAND ----------

enc_features_df = compute_enc_features(spark.table('riley_e2e.encounters'))

#Note: You might need to delete the FS table using the UI
enc_feature_table = fs.create_table(
  name=f'{dbName}.enc_features',
  primary_keys=['ENCOUNTER_ID'],
  df=enc_features_df,
  description='Base and derived features from the Encounter Table'
# TODO add path arg to create_table()
#   , path='...'
)

fs.write_table(df=enc_features_df, name=f'{dbName}.enc_features', mode='merge')

# COMMAND ----------

def compute_age_at_enc(encounters, patients):
  return (
    encounters
    .join(patients, patients['id'] == encounters['PATIENT'])
    .select(
      encounters.Id.alias('encounter_id'),
      patients.Id.alias('patient_id'),
      ((f.datediff(col('START'), col('BIRTHDATE'))) / 365.25).alias('age_at_encounter')
    )
  )

# COMMAND ----------

aae_features_df = compute_age_at_enc(spark.table('riley_e2e.encounters'),spark.table('riley_e2e.patients'))

#Note: You might need to delete the FS table using the UI
aae_feature_table = fs.create_table(
  name=f'{dbName}.age_at_enc_features',
  primary_keys=['encounter_id'],
  df=aae_features_df,
  description='determine the age of the patient at the time of the encounter'
# TODO add path arg to create_table()
#   , path='...'
)

fs.write_table(df=aae_features_df, name=f'{dbName}.age_at_enc_features', mode='merge')

# COMMAND ----------

# %sql
# drop table riley_e2e.pat_features;
# -- drop table riley_e2e.enc_features;
# -- drop table riley_e2e.age_at_enc_features;


# COMMAND ----------


