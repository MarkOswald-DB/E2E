# Databricks notebook source
from pyspark.sql.types import *
import pyspark.sql.functions as f

dbutils.widgets.text('synth_out','/FileStore/hls/synthea/data')
synth_out=dbutils.widgets.get('synth_out')
dbutils.widgets.text('demo_path','/FileStore/hls/synthea/data')
demo_path=dbutils.widgets.get('demo_path')
dbutils.widgets.text('db_name','riley_e2e')
db_name=dbutils.widgets.get('db_name')
dbutils.widgets.dropdown('mode','update',['update','init'])
mode=dbutils.widgets.get('mode')

dbutils.widgets.text('update_months','1')
update_months=int(dbutils.widgets.get('update_months'))


synth_out, demo_path
spark.sql(f'create database if not exists {db_name}')
spark.sql(f'use {db_name}')



source_path = demo_path+'/source'
landed_path = demo_path+'/landed'

table_dependencies = ['encounters', 'patients', 'conditions', 'procedures', 'observations', 'medications', 'immunizations']

schemas = {
  'encounters':
    {'fields': [{'metadata': {}, 'name': 'Id', 'nullable': True, 'type': 'string'},
      {'metadata': {}, 'name': 'START', 'nullable': True, 'type': 'timestamp'},
      {'metadata': {}, 'name': 'STOP', 'nullable': True, 'type': 'timestamp'},
      {'metadata': {}, 'name': 'PATIENT', 'nullable': True, 'type': 'string'},
      {'metadata': {}, 'name': 'ORGANIZATION', 'nullable': True, 'type': 'string'},
      {'metadata': {}, 'name': 'PROVIDER', 'nullable': True, 'type': 'string'},
      {'metadata': {}, 'name': 'PAYER', 'nullable': True, 'type': 'string'},
      {'metadata': {},
       'name': 'ENCOUNTERCLASS',
       'nullable': True,
       'type': 'string'},
      {'metadata': {}, 'name': 'CODE', 'nullable': True, 'type': 'integer'},
      {'metadata': {}, 'name': 'DESCRIPTION', 'nullable': True, 'type': 'string'},
      {'metadata': {},
       'name': 'BASE_ENCOUNTER_COST',
       'nullable': True,
       'type': 'float'},
      {'metadata': {},
       'name': 'TOTAL_CLAIM_COST',
       'nullable': True,
       'type': 'float'},
      {'metadata': {},
       'name': 'PAYER_COVERAGE',
       'nullable': True,
       'type': 'float'},
      {'metadata': {}, 'name': 'REASONCODE', 'nullable': True, 'type': 'integer'},
      {'metadata': {},
       'name': 'REASONDESCRIPTION',
       'nullable': True,
       'type': 'string'}],
     'type': 'struct'}, 
  'patients':
    {'fields': [{'metadata': {}, 'name': 'Id', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'BIRTHDATE', 'nullable': True, 'type': 'date'},
    {'metadata': {}, 'name': 'DEATHDATE', 'nullable': True, 'type': 'date'},
    {'metadata': {}, 'name': 'SSN', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'DRIVERS', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'PASSPORT', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'PREFIX', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'FIRST', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'LAST', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'SUFFIX', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'MAIDEN', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'MARITAL', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'RACE', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'ETHNICITY', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'GENDER', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'BIRTHPLACE', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'ADDRESS', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'CITY', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'STATE', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'COUNTY', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'ZIP', 'nullable': True, 'type': 'integer'},
    {'metadata': {}, 'name': 'LAT', 'nullable': True, 'type': 'float'},
    {'metadata': {}, 'name': 'LON', 'nullable': True, 'type': 'float'},
    {'metadata': {},
     'name': 'HEALTHCARE_EXPENSES',
     'nullable': True,
     'type': 'float'},
    {'metadata': {},
     'name': 'HEALTHCARE_COVERAGE',
     'nullable': True,
     'type': 'float'},
    {'metadata': {}, 'name': 'INCOME', 'nullable': True, 'type': 'float'}],
   'type': 'struct'},
  'conditions':
    {'fields': [{'metadata': {},
     'name': 'START',
     'nullable': True,
     'type': 'date'},
    {'metadata': {}, 'name': 'STOP', 'nullable': True, 'type': 'date'},
    {'metadata': {}, 'name': 'PATIENT', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'ENCOUNTER', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'CODE', 'nullable': True, 'type': 'integer'},
    {'metadata': {}, 'name': 'DESCRIPTION', 'nullable': True, 'type': 'string'}],
   'type': 'struct'}, 
  'procedures':
    {'fields': [{'metadata': {},
      'name': 'START',
      'nullable': True,
      'type': 'timestamp'},
    {'metadata': {}, 'name': 'STOP', 'nullable': True, 'type': 'timestamp'},
    {'metadata': {}, 'name': 'PATIENT', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'ENCOUNTER', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'CODE', 'nullable': True, 'type': 'integer'},
    {'metadata': {}, 'name': 'DESCRIPTION', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'BASE_COST', 'nullable': True, 'type': 'float'},
    {'metadata': {}, 'name': 'REASONCODE', 'nullable': True, 'type': 'integer'},
    {'metadata': {},
    'name': 'REASONDESCRIPTION',
    'nullable': True,
    'type': 'string'}],
    'type': 'struct'}, 
  'observations':
    {'fields': [{'metadata': {},
      'name': 'DATE',
      'nullable': True,
      'type': 'timestamp'},
    {'metadata': {}, 'name': 'PATIENT', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'ENCOUNTER', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'CATEGORY', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'CODE', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'DESCRIPTION', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'VALUE', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'UNITS', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'TYPE', 'nullable': True, 'type': 'string'}],
    'type': 'struct'}, 
  'medications':
    {'fields': [{'metadata': {},
     'name': 'START',
     'nullable': True,
     'type': 'timestamp'},
    {'metadata': {}, 'name': 'STOP', 'nullable': True, 'type': 'timestamp'},
    {'metadata': {}, 'name': 'PATIENT', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'PAYER', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'ENCOUNTER', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'CODE', 'nullable': True, 'type': 'integer'},
    {'metadata': {}, 'name': 'DESCRIPTION', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'BASE_COST', 'nullable': True, 'type': 'float'},
    {'metadata': {},
     'name': 'PAYER_COVERAGE',
     'nullable': True,
     'type': 'float'},
    {'metadata': {}, 'name': 'DISPENSES', 'nullable': True, 'type': 'integer'},
    {'metadata': {}, 'name': 'TOTALCOST', 'nullable': True, 'type': 'float'},
    {'metadata': {}, 'name': 'REASONCODE', 'nullable': True, 'type': 'integer'},
    {'metadata': {},
     'name': 'REASONDESCRIPTION',
     'nullable': True,
     'type': 'string'}],
   'type': 'struct'}, 
  'immunizations':
    {'fields': [{'metadata': {},
     'name': 'DATE',
     'nullable': True,
     'type': 'timestamp'},
    {'metadata': {}, 'name': 'PATIENT', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'ENCOUNTER', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'CODE', 'nullable': True, 'type': 'integer'},
    {'metadata': {}, 'name': 'DESCRIPTION', 'nullable': True, 'type': 'string'},
    {'metadata': {}, 'name': 'BASE_COST', 'nullable': True, 'type': 'float'}],
   'type': 'struct'}
}

# COMMAND ----------

if mode == 'init':
  dbutils.fs.rm(landed_path,True)
  dbutils.fs.rm(source_path,True)
  spark.sql(f'drop database if exists {db_name} cascade')

# COMMAND ----------

for table in table_dependencies:
  dbutils.fs.mkdirs(landed_path+f'/{table}')

# COMMAND ----------

#todo fix this path
tables_path = synth_out+'/congestive_heart_failure/csv/2022_07_28T00_10_47Z'
table_files = dbutils.fs.ls(tables_path)
display(table_files)

# COMMAND ----------

for table in table_files:
  table_name = table.name.replace('.csv','')
  if table_name in table_dependencies:
    spark.read.schema(StructType.fromJson(schemas[table_name])).csv(table.path, header=True).createOrReplaceTempView(table_name)

# COMMAND ----------

encounters_path = source_path+'/encounters'
if mode == 'init':
  (
    spark.table('encounters')
    .withColumn('START_MONTH', f.date_trunc('mm', 'START'))
    .write.format('parquet')
    #TODO partition by STOP
    .partitionBy('START_MONTH')
    .mode('overwrite')
    .save(encounters_path)
  )

# COMMAND ----------

encounters_landed_path = landed_path+'/encounters'

# COMMAND ----------

def update_encounters(update_months):
  source_files = sorted([x.path for x in dbutils.fs.ls(encounters_path)])

  dbutils.fs.mkdirs(encounters_landed_path)
  landed_files = [x.path for x in dbutils.fs.ls(encounters_landed_path)]
  if len(landed_files) == 0:
    i = 0
  else:
    # +1 because I want to know the next file to movve
    i = source_files.index(max(landed_files).replace(landed_path, source_path))+1

  for path in source_files[i:i+update_months]:
    dbutils.fs.cp(path, path.replace(source_path, landed_path), recurse=True)

update_encounters(update_months)

# COMMAND ----------

def update_patients():
  spark.read.parquet(encounters_landed_path).createOrReplaceTempView('encounters_incrememntal')

  spark.read.schema(StructType.fromJson(schemas['patients'])).parquet(landed_path+'/patients').createOrReplaceTempView('patients_incrememntal')

  df = spark.sql(
  """select * from patients
  where patients.id in (select distinct PATIENT from encounters_incrememntal)
  and patients.id not in (select patients_incrememntal.id from patients_incrememntal)"""
  )

  df.write.mode('append').parquet(landed_path+'/patients')
update_patients()

# COMMAND ----------

(spark.createDataFrame(dbutils.fs.ls(landed_path+'/patients'))
 .withColumn('modificationTime', f.from_unixtime(f.col('modificationTime')/1000))
).display()

# COMMAND ----------

landed_path

# COMMAND ----------


def update_enc_based_table(table, schema):
  spark.read.parquet(encounters_landed_path).createOrReplaceTempView('encounters_incrememntal')

  spark.read.schema(spark.table(table).schema).parquet(landed_path+f'/{table}').createOrReplaceTempView(f'{table}_incrememntal')

  df = spark.sql(
  f"""select * from {table}
  where {table}.ENCOUNTER in (select distinct id from encounters_incrememntal)
  and {table}.ENCOUNTER not in (select distinct ENCOUNTER from {table}_incrememntal)
  """
  ).coalesce(1)

  df.write.mode('append').parquet(landed_path+f'/{table}')

  

for table in table_dependencies:
  if table not in ['encounters', 'patients']:
    print(table)
    update_enc_based_table(table, schemas[table])

# COMMAND ----------

enc_based_tables = ['conditions', 'procedures', 'observations', 'medications', 'immunizations']
