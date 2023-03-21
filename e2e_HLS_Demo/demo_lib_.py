# Databricks notebook source
# MAGIC %sql
# MAGIC select * from 

# COMMAND ----------

import dlt

# COMMAND ----------

from pyspark.sql.functions import col, lit
import pyspark.sql.functions as f
def get_raw_data(encounters):
  
  return(
    encounters
    .where(col('encounterclass') == 'inpatient')
    .select(
      col('patient'),
      col('encounterclass'),
      col('start').alias('EVENT_DATE'),
      lit(-1).alias('EVENT_TYPE'),
      f.expr('ROW_NUMBER () OVER (PARTITION BY patient, encounterclass ORDER BY start, stop) AS START_ORDINAL')
    )
  ).union(
    encounters
    .where(col('encounterclass') == 'inpatient')
    .select(
      col('patient'),
      col('encounterclass'),
      f.date_add(col('stop'),1),
      lit(1).alias('EVENT_TYPE'),
      lit(None)
  ))

def get_e(RAWDATA):
  return (RAWDATA
  .select(
    col('patient'),
    col('encounterclass'),
    col('EVENT_DATE'),
    col('EVENT_TYPE'),
    f.expr('MAX(START_ORDINAL) OVER (PARTITION BY patient, encounterclass ORDER BY EVENT_DATE, EVENT_TYPE ROWS UNBOUNDED PRECEDING ) AS START_ORDINAL'),
    f.expr('ROW_NUMBER() OVER (PARTITION BY patient, encounterclass ORDER BY EVENT_DATE, EVENT_TYPE ) AS OVERALL_ORD')
  ))

def get_CTE_END_DATES(E):
  return (
    E
    .filter( 2* col('START_ORDINAL') - col('OVERALL_ORD') == 0)
    .select(
      col('patient'),
      col('encounterclass'),
      f.date_add(col('EVENT_DATE'), -1).alias('END_DATE')
    )
  )

def get_CTE_VISIT_ENDS(encounters, CTE_END_DATES):
  V = encounters.alias('V')
  E = CTE_END_DATES.alias('E')
  return (
    V
    .join(E, [col('V.PATIENT')==col('E.patient'), col('V.ENCOUNTERCLASS')==col('E.encounterclass'), col('E.END_DATE') >= col('V.START')])
    .groupby(V.PATIENT, V.ENCOUNTERCLASS, V.START)
    .agg(
      f.min(V.Id).alias('encounter_id'),
      f.min(E.END_DATE).alias('VISIT_END_DATE')
    )
    .select(
      col('encounter_id'),
      col('PATIENT'),
      col('ENCOUNTERCLASS'),
      col('START').alias('VISIT_START_DATE'),
      col('VISIT_END_DATE')
    )
    
  )

def get_IP_VISITS(CTE_VISIT_ENDS):
  return (
    CTE_VISIT_ENDS
    .groupby('encounter_id', 'patient', 'encounterclass', 'VISIT_END_DATE')
    .agg(
      f.min('VISIT_START_DATE').alias('VISIT_START_DATE')
    )
    .select(
      col('encounter_id'),
      col('patient'),
      col('encounterclass'),
      col('VISIT_START_DATE'),
      col('VISIT_END_DATE'),
    )
  )
  
def get_T1(encounters):
  return(
    encounters.alias('CL1')
    .filter(col('encounterclass').isin(['emergency', 'urgent']))
    .join(encounters.alias('CL2'), [col('CL1.patient') == col('CL2.patient'), 
                                    col('CL1.start') == col('CL2.start'), 
                                    col('CL1.encounterclass') == col('CL2.encounterclass')])
    .select(
      col('CL1.id').alias('encounter_id'),
      col('CL1.patient'),
      col('CL1.encounterclass'),
      col('CL1.start').alias('VISIT_START_DATE'),
      col('CL2.stop').alias('VISIT_END_DATE')
    )
  )


def get_ER_VISITS(T1):
  return(
    T1
    .groupby('patient', 'encounterclass', 'VISIT_START_DATE')
    .agg(
      f.min('encounter_id').alias('encounter_id'),
      col('patient'),
      col('encounterclass'),
      col('VISIT_START_DATE'),
      f.max('VISIT_END_DATE').alias('VISIT_END_DATE')
    )
    .select(
      col('encounter_id'),
      col('patient'),
      col('encounterclass'),
      col('VISIT_START_DATE'),
      col('VISIT_END_DATE')
    )
  )
  
def get_CTE_VISITS_DISTINCT(encounters):
  return (
    encounters
    .filter(col('encounterclass').isin(['ambulatory', 'wellness', 'outpatient']))
    .groupby('patient', 'encounterclass', 'start', 'stop')
    .agg(f.min('id').alias('encounter_id'))
    .select(
      col('encounter_id'),
      col('patient'),
      col('encounterclass'),
      col('start').alias('VISIT_START_DATE'),
      col('stop').alias('VISIT_END_DATE')
    )
  )

def get_OP_VISITS(CTE_VISITS_DISTINCT):
  return (
    CTE_VISITS_DISTINCT
    .groupby(
      'patient',
      'encounterclass',
      'VISIT_START_DATE'
    )
    .agg(
      f.min('encounter_id').alias('encounter_id'),
      f.max('VISIT_END_DATE').alias('VISIT_END_DATE')
    )
    .select(
    col('encounter_id'),
    col('patient'),
    col('encounterclass'),
    col('VISIT_START_DATE'),
    col('VISIT_END_DATE')
    )
  )
