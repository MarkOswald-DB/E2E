# Databricks notebook source
#%pip install Workspace/Repos/riley.rustad@databricks.com/field-demo-riley/demo-HLS/e2e_HLS_Demo/demo_lib.py

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/riley.rustad@databricks.com/field-demo-riley/demo-HLS/e2e_HLS_Demo")

# COMMAND ----------

import dlt

# COMMAND ----------

from pyspark.sql.functions import col, expr,lit
import pyspark.sql.functions as f
from pyspark.sql.types import *
from demo_lib import *


# COMMAND ----------

import os
os.listdir("/Workspace/Repos/riley.rustad@databricks.com/field-demo-riley/demo-HLS/e2e_HLS_Demo/")

# COMMAND ----------

get_raw_data

# COMMAND ----------

# code_location = spark.conf.get("mypipeline.code_location")
# import sys
# sys.path.append(f"/Workspace{code_location}")

# COMMAND ----------

#%run ./demo_lib

# COMMAND ----------

# RAWDATA = get_raw_data(encounters)
# E = get_e(RAWDATA)
# CTE_END_DATES = get_CTE_END_DATES(E)
# CTE_VISIT_ENDS = get_CTE_VISIT_ENDS(encounters, CTE_END_DATES)
# IP_VISITS =  get_IP_VISITS(CTE_VISIT_ENDS)
# IP_VISITS.display()

# COMMAND ----------

#todo parameterize the bronze database


# COMMAND ----------

@dlt.view()
def IP_VISITS():
  RAWDATA = get_raw_data(spark.table('LIVE.encounters'))
  E = get_e(RAWDATA)
  CTE_END_DATES = get_CTE_END_DATES(E)
  CTE_VISIT_ENDS = get_CTE_VISIT_ENDS(spark.table('LIVE.encounters'), CTE_END_DATES)
  df =  get_IP_VISITS(CTE_VISIT_ENDS)
  return df

# COMMAND ----------

@dlt.view()
def ER_VISITS():
  T1 = get_T1(spark.table('LIVE.encounters'))
  df = get_ER_VISITS(T1)
  return df

# COMMAND ----------

@dlt.view()
def OP_VISITS():
  CTE_VISITS_DISTINCT = get_CTE_VISITS_DISTINCT(spark.table('LIVE.encounters'))
  # CTE_VISITS_DISTINCT.collect()
  df = get_OP_VISITS(CTE_VISITS_DISTINCT)
  return df

# COMMAND ----------

@dlt.table(name='ALL_VISITS')
def get_all_visits():
  return (
    spark.table('LIVE.IP_VISITS')
    .union(spark.table('LIVE.ER_VISITS'))
    .union(spark.table('LIVE.OP_VISITS'))
    .select(
      '*',
      f.expr('ROW_NUMBER() OVER (ORDER BY patient) as visit_occurrence_id')
    )
  )
