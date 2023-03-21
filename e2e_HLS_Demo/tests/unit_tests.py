# Databricks notebook source
# from runtime.nutterfixture import NutterFixture, tag
import pytest, json
from pyspark.sql.types import *
from pyspark.sql.types import Row
from datetime import timedelta
import datetime

# COMMAND ----------

# MAGIC %run ../demo_lib

# COMMAND ----------

# @pytest.fixture
def encounters():
  og_date = datetime.datetime.strptime('2022/01/01 00:00:00', '%Y/%m/%d %H:%M:%S')
  og_date+timedelta(days=1)
  return spark.createDataFrame(
    [
      # Inpatient Examples
      # Same patient inpatient twice - should produce 1 and 2
      ['2', og_date+timedelta(days=1), og_date+timedelta(days=2), '1', '1', '1', '1', 'inpatient', 1, 'foo', 1.0, 1.0, 1.0, 1, 'foo'],
      ['3', og_date+timedelta(days=2), og_date+timedelta(days=3), '1', '1', '1', '1', 'inpatient', 1, 'foo', 1.0, 1.0, 1.0, 1, 'foo'],
      ['4', og_date+timedelta(days=3), og_date+timedelta(days=4), '1', '1', '1', '1', 'inpatient', 1, 'foo', 1.0, 1.0, 1.0, 1, 'foo'],
      # Same as above for another patient
      ['5', og_date+timedelta(days=4), og_date+timedelta(days=5), '2', '1', '1', '1', 'inpatient', 1, 'foo', 1.0, 1.0, 1.0, 1, 'foo'],
      ['6', og_date+timedelta(days=5), og_date+timedelta(days=6), '2', '1', '1', '1', 'inpatient', 1, 'foo', 1.0, 1.0, 1.0, 1, 'foo'],
      ['7', og_date+timedelta(days=6), og_date+timedelta(days=7), '2', '1', '1', '1', 'inpatient', 1, 'foo', 1.0, 1.0, 1.0, 1, 'foo'],
      
      # ER Examples
      # first patient
      ['8', og_date+timedelta(days=1), og_date+timedelta(days=2), '3', '1', '1', '1', 'urgent', 1, 'foo', 1.0, 1.0, 1.0, 1, 'foo'],
      # Case where two encounters have the same start time
      ['9', og_date+timedelta(days=2), og_date+timedelta(days=3), '3', '1', '1', '1', 'emergency', 1, 'foo', 1.0, 1.0, 1.0, 1, 'foo'],
      ['10', og_date+timedelta(days=2), og_date+timedelta(days=4), '3', '1', '1', '1', 'emergency', 1, 'foo', 1.0, 1.0, 1.0, 1, 'foo'],
      # Same as above for another patient
      ['11', og_date+timedelta(days=4), og_date+timedelta(days=5), '4', '1', '1', '1', 'emergency', 1, 'foo', 1.0, 1.0, 1.0, 1, 'foo'],
      ['12', og_date+timedelta(days=5), og_date+timedelta(days=6), '4', '1', '1', '1', 'emergency', 1, 'foo', 1.0, 1.0, 1.0, 1, 'foo'],
      ['13', og_date+timedelta(days=6), og_date+timedelta(days=7), '4', '1', '1', '1', 'urgent', 1, 'foo', 1.0, 1.0, 1.0, 1, 'foo'],
      
      # OP Examples
      # first patient
      # First two examples are two encounters with same start and stop and encounter type
      ['14', og_date+timedelta(days=1), og_date+timedelta(days=2), '3', '1', '1', '1', 'ambulatory', 1, 'foo', 1.0, 1.0, 1.0, 1, 'foo'],
      ['15', og_date+timedelta(days=1), og_date+timedelta(days=2), '3', '1', '1', '1', 'ambulatory', 1, 'foo', 1.0, 1.0, 1.0, 1, 'foo'],
      ['16', og_date+timedelta(days=2), og_date+timedelta(days=3), '3', '1', '1', '1', 'wellness', 1, 'foo', 1.0, 1.0, 1.0, 1, 'foo'],
      ['17', og_date+timedelta(days=2), og_date+timedelta(days=4), '3', '1', '1', '1', 'outpatient', 1, 'foo', 1.0, 1.0, 1.0, 1, 'foo'],
      # Same as above for another patient
      ['18', og_date+timedelta(days=4), og_date+timedelta(days=5), '4', '1', '1', '1', 'ambulatory', 1, 'foo', 1.0, 1.0, 1.0, 1, 'foo'],
      ['19', og_date+timedelta(days=5), og_date+timedelta(days=6), '4', '1', '1', '1', 'ambulatory', 1, 'foo', 1.0, 1.0, 1.0, 1, 'foo'],
      ['20', og_date+timedelta(days=6), og_date+timedelta(days=7), '4', '1', '1', '1', 'outpatient', 1, 'foo', 1.0, 1.0, 1.0, 1, 'foo'],
    ],
    schema = StructType.fromJson({'fields': [{'metadata': {}, 'name': 'Id', 'nullable': True, 'type': 'string'},
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
     'type': 'struct'})
  )

# COMMAND ----------

def test_get_raw_data(encounters):
  expected_output = (
    #Rows for first patient in correct row order with event type -1
    Row(patient='1', encounterclass='inpatient', EVENT_DATE=datetime.datetime(2022, 1, 2, 0, 0), EVENT_TYPE=-1, START_ORDINAL=1),
    Row(patient='1', encounterclass='inpatient', EVENT_DATE=datetime.datetime(2022, 1, 3, 0, 0), EVENT_TYPE=-1, START_ORDINAL=2),
    Row(patient='1', encounterclass='inpatient', EVENT_DATE=datetime.datetime(2022, 1, 4, 0, 0), EVENT_TYPE=-1, START_ORDINAL=3),
    #Rows for second patient in correct row order
    Row(patient='2', encounterclass='inpatient', EVENT_DATE=datetime.datetime(2022, 1, 5, 0, 0), EVENT_TYPE=-1, START_ORDINAL=1),
    Row(patient='2', encounterclass='inpatient', EVENT_DATE=datetime.datetime(2022, 1, 6, 0, 0), EVENT_TYPE=-1, START_ORDINAL=2),
    Row(patient='2', encounterclass='inpatient', EVENT_DATE=datetime.datetime(2022, 1, 7, 0, 0), EVENT_TYPE=-1, START_ORDINAL=3),
    # Rows for event type 1,  no ordinal
    Row(patient='1', encounterclass='inpatient', EVENT_DATE=datetime.datetime(2022, 1, 4, 0, 0), EVENT_TYPE=1, START_ORDINAL=None),
    Row(patient='1', encounterclass='inpatient', EVENT_DATE=datetime.datetime(2022, 1, 5, 0, 0), EVENT_TYPE=1, START_ORDINAL=None),
    Row(patient='1', encounterclass='inpatient', EVENT_DATE=datetime.datetime(2022, 1, 6, 0, 0), EVENT_TYPE=1, START_ORDINAL=None),
    
    Row(patient='2', encounterclass='inpatient', EVENT_DATE=datetime.datetime(2022, 1, 7, 0, 0), EVENT_TYPE=1, START_ORDINAL=None),
    Row(patient='2', encounterclass='inpatient', EVENT_DATE=datetime.datetime(2022, 1, 8, 0, 0), EVENT_TYPE=1, START_ORDINAL=None),
    Row(patient='2', encounterclass='inpatient', EVENT_DATE=datetime.datetime(2022, 1, 9, 0, 0), EVENT_TYPE=1, START_ORDINAL=None))
    
  assert sorted(get_raw_data(encounters).collect()) == sorted(expected_output)


# COMMAND ----------

def test_get_e(encounters):
  RAWDATA = get_raw_data(encounters)
  expected_output =[
    # first patient event type -1 should be the same start ordinal
    Row(patient='1', encounterclass='inpatient', EVENT_DATE=datetime.datetime(2022, 1, 2, 0, 0), EVENT_TYPE=-1, START_ORDINAL=1, OVERALL_ORD=1),
    Row(patient='1', encounterclass='inpatient', EVENT_DATE=datetime.datetime(2022, 1, 3, 0, 0), EVENT_TYPE=-1, START_ORDINAL=2, OVERALL_ORD=2),
    Row(patient='1', encounterclass='inpatient', EVENT_DATE=datetime.datetime(2022, 1, 4, 0, 0), EVENT_TYPE=-1, START_ORDINAL=3, OVERALL_ORD=3),
    # first patient event type 1 should be max start ordinal
    Row(patient='1', encounterclass='inpatient', EVENT_DATE=datetime.datetime(2022, 1, 4, 0, 0), EVENT_TYPE=1, START_ORDINAL=3, OVERALL_ORD=4),
    Row(patient='1', encounterclass='inpatient', EVENT_DATE=datetime.datetime(2022, 1, 5, 0, 0), EVENT_TYPE=1, START_ORDINAL=3, OVERALL_ORD=5),
    Row(patient='1', encounterclass='inpatient', EVENT_DATE=datetime.datetime(2022, 1, 6, 0, 0), EVENT_TYPE=1, START_ORDINAL=3, OVERALL_ORD=6),
    # same below for second patient
    Row(patient='2', encounterclass='inpatient', EVENT_DATE=datetime.datetime(2022, 1, 5, 0, 0), EVENT_TYPE=-1, START_ORDINAL=1, OVERALL_ORD=1),
    Row(patient='2', encounterclass='inpatient', EVENT_DATE=datetime.datetime(2022, 1, 6, 0, 0), EVENT_TYPE=-1, START_ORDINAL=2, OVERALL_ORD=2),
    Row(patient='2', encounterclass='inpatient', EVENT_DATE=datetime.datetime(2022, 1, 7, 0, 0), EVENT_TYPE=-1, START_ORDINAL=3, OVERALL_ORD=3),
    Row(patient='2', encounterclass='inpatient', EVENT_DATE=datetime.datetime(2022, 1, 7, 0, 0), EVENT_TYPE=1, START_ORDINAL=3, OVERALL_ORD=4),
    Row(patient='2', encounterclass='inpatient', EVENT_DATE=datetime.datetime(2022, 1, 8, 0, 0), EVENT_TYPE=1, START_ORDINAL=3, OVERALL_ORD=5),
    Row(patient='2', encounterclass='inpatient', EVENT_DATE=datetime.datetime(2022, 1, 9, 0, 0), EVENT_TYPE=1, START_ORDINAL=3, OVERALL_ORD=6)]
    
  assert sorted(get_e(RAWDATA).collect()) == sorted(expected_output)

# COMMAND ----------

def test_get_CTE_END_DATES(encounters):
  RAWDATA = get_raw_data(encounters)
  E = get_e(RAWDATA)
  
  expected_output=[
    Row(patient='1', encounterclass='inpatient', END_DATE=datetime.date(2022, 1, 5)),
    Row(patient='2', encounterclass='inpatient', END_DATE=datetime.date(2022, 1, 8))]
  
  assert sorted(get_CTE_END_DATES(E).collect()) == sorted(expected_output)

# COMMAND ----------

def test_get_CTE_VISIT_ENDS(encounters):
  RAWDATA = get_raw_data(encounters)
  E = get_e(RAWDATA)
  CTE_END_DATES = get_CTE_END_DATES(E)
  
  expected_output=[
    Row(encounter_id='2', PATIENT='1', ENCOUNTERCLASS='inpatient', VISIT_START_DATE=datetime.datetime(2022, 1, 2, 0, 0), VISIT_END_DATE=datetime.date(2022, 1, 5)),
    Row(encounter_id='3', PATIENT='1', ENCOUNTERCLASS='inpatient', VISIT_START_DATE=datetime.datetime(2022, 1, 3, 0, 0), VISIT_END_DATE=datetime.date(2022, 1, 5)),
    Row(encounter_id='4', PATIENT='1', ENCOUNTERCLASS='inpatient', VISIT_START_DATE=datetime.datetime(2022, 1, 4, 0, 0), VISIT_END_DATE=datetime.date(2022, 1, 5)),
    Row(encounter_id='5', PATIENT='2', ENCOUNTERCLASS='inpatient', VISIT_START_DATE=datetime.datetime(2022, 1, 5, 0, 0), VISIT_END_DATE=datetime.date(2022, 1, 8)),
    Row(encounter_id='6', PATIENT='2', ENCOUNTERCLASS='inpatient', VISIT_START_DATE=datetime.datetime(2022, 1, 6, 0, 0), VISIT_END_DATE=datetime.date(2022, 1, 8)),
    Row(encounter_id='7', PATIENT='2', ENCOUNTERCLASS='inpatient', VISIT_START_DATE=datetime.datetime(2022, 1, 7, 0, 0), VISIT_END_DATE=datetime.date(2022, 1, 8))]
  
  assert sorted(get_CTE_VISIT_ENDS(encounters, CTE_END_DATES).collect()) == sorted(expected_output)

# COMMAND ----------

def test_get_IP_VISITS(encounters):
  RAWDATA = get_raw_data(encounters)
  E = get_e(RAWDATA)
  CTE_END_DATES = get_CTE_END_DATES(E)
  CTE_VISIT_ENDS = get_CTE_VISIT_ENDS(encounters, CTE_END_DATES)
  
  expected_output=[
    Row(encounter_id='2', patient='1', encounterclass='inpatient', VISIT_START_DATE=datetime.datetime(2022, 1, 2, 0, 0), VISIT_END_DATE=datetime.date(2022, 1, 5)),
    Row(encounter_id='3', patient='1', encounterclass='inpatient', VISIT_START_DATE=datetime.datetime(2022, 1, 3, 0, 0), VISIT_END_DATE=datetime.date(2022, 1, 5)),
    Row(encounter_id='4', patient='1', encounterclass='inpatient', VISIT_START_DATE=datetime.datetime(2022, 1, 4, 0, 0), VISIT_END_DATE=datetime.date(2022, 1, 5)),
    Row(encounter_id='5', patient='2', encounterclass='inpatient', VISIT_START_DATE=datetime.datetime(2022, 1, 5, 0, 0), VISIT_END_DATE=datetime.date(2022, 1, 8)),
    Row(encounter_id='6', patient='2', encounterclass='inpatient', VISIT_START_DATE=datetime.datetime(2022, 1, 6, 0, 0), VISIT_END_DATE=datetime.date(2022, 1, 8)),
    Row(encounter_id='7', patient='2', encounterclass='inpatient', VISIT_START_DATE=datetime.datetime(2022, 1, 7, 0, 0), VISIT_END_DATE=datetime.date(2022, 1, 8))]
  
  assert sorted(get_IP_VISITS(CTE_VISIT_ENDS).collect()) == sorted(expected_output)

# COMMAND ----------

def test_get_T1(encounters):
  expected_output=[
    Row(encounter_id='8', patient='3', encounterclass='urgent', VISIT_START_DATE=datetime.datetime(2022, 1, 2, 0, 0), VISIT_END_DATE=datetime.datetime(2022, 1, 3, 0, 0)),
 Row(encounter_id='9', patient='3', encounterclass='emergency', VISIT_START_DATE=datetime.datetime(2022, 1, 3, 0, 0), VISIT_END_DATE=datetime.datetime(2022, 1, 4, 0, 0)),
 Row(encounter_id='9', patient='3', encounterclass='emergency', VISIT_START_DATE=datetime.datetime(2022, 1, 3, 0, 0), VISIT_END_DATE=datetime.datetime(2022, 1, 5, 0, 0)),
 Row(encounter_id='10', patient='3', encounterclass='emergency', VISIT_START_DATE=datetime.datetime(2022, 1, 3, 0, 0), VISIT_END_DATE=datetime.datetime(2022, 1, 4, 0, 0)),
 Row(encounter_id='10', patient='3', encounterclass='emergency', VISIT_START_DATE=datetime.datetime(2022, 1, 3, 0, 0), VISIT_END_DATE=datetime.datetime(2022, 1, 5, 0, 0)),
 Row(encounter_id='11', patient='4', encounterclass='emergency', VISIT_START_DATE=datetime.datetime(2022, 1, 5, 0, 0), VISIT_END_DATE=datetime.datetime(2022, 1, 6, 0, 0)),
 Row(encounter_id='12', patient='4', encounterclass='emergency', VISIT_START_DATE=datetime.datetime(2022, 1, 6, 0, 0), VISIT_END_DATE=datetime.datetime(2022, 1, 7, 0, 0)),
 Row(encounter_id='13', patient='4', encounterclass='urgent', VISIT_START_DATE=datetime.datetime(2022, 1, 7, 0, 0), VISIT_END_DATE=datetime.datetime(2022, 1, 8, 0, 0))]
  
  assert sorted(get_T1(encounters).collect()) == sorted(expected_output)

# COMMAND ----------

def test_get_ER_VISITS(encounters):
  T1 = get_T1(encounters)
  
  expected_output=[
    Row(encounter_id='8', patient='3', encounterclass='urgent', VISIT_START_DATE=datetime.datetime(2022, 1, 2, 0, 0), VISIT_END_DATE=datetime.datetime(2022, 1, 3, 0, 0)),
    # note that we skipeed 9 because they had the same start date
    Row(encounter_id='10', patient='3', encounterclass='emergency', VISIT_START_DATE=datetime.datetime(2022, 1, 3, 0, 0), VISIT_END_DATE=datetime.datetime(2022, 1, 5, 0, 0)),
    Row(encounter_id='11', patient='4', encounterclass='emergency', VISIT_START_DATE=datetime.datetime(2022, 1, 5, 0, 0), VISIT_END_DATE=datetime.datetime(2022, 1, 6, 0, 0)),
    Row(encounter_id='12', patient='4', encounterclass='emergency', VISIT_START_DATE=datetime.datetime(2022, 1, 6, 0, 0), VISIT_END_DATE=datetime.datetime(2022, 1, 7, 0, 0)),
    Row(encounter_id='13', patient='4', encounterclass='urgent', VISIT_START_DATE=datetime.datetime(2022, 1, 7, 0, 0), VISIT_END_DATE=datetime.datetime(2022, 1, 8, 0, 0))]
  
  assert sorted(get_ER_VISITS(T1).collect()) == sorted(expected_output)

# COMMAND ----------

def test_get_CTE_VISITS_DISTINCT(encounters):
  expected_output=[
    Row(encounter_id='14', patient='3', encounterclass='ambulatory', VISIT_START_DATE=datetime.datetime(2022, 1, 2, 0, 0), VISIT_END_DATE=datetime.datetime(2022, 1, 3, 0, 0)),
    # notice that 15 is missing because it was 2 encounters of the same type for the same visit - use min encounter id
    Row(encounter_id='16', patient='3', encounterclass='wellness', VISIT_START_DATE=datetime.datetime(2022, 1, 3, 0, 0), VISIT_END_DATE=datetime.datetime(2022, 1, 4, 0, 0)),
    Row(encounter_id='17', patient='3', encounterclass='outpatient', VISIT_START_DATE=datetime.datetime(2022, 1, 3, 0, 0), VISIT_END_DATE=datetime.datetime(2022, 1, 5, 0, 0)),
    Row(encounter_id='18', patient='4', encounterclass='ambulatory', VISIT_START_DATE=datetime.datetime(2022, 1, 5, 0, 0), VISIT_END_DATE=datetime.datetime(2022, 1, 6, 0, 0)),
    Row(encounter_id='19', patient='4', encounterclass='ambulatory', VISIT_START_DATE=datetime.datetime(2022, 1, 6, 0, 0), VISIT_END_DATE=datetime.datetime(2022, 1, 7, 0, 0)),
    Row(encounter_id='20', patient='4', encounterclass='outpatient', VISIT_START_DATE=datetime.datetime(2022, 1, 7, 0, 0), VISIT_END_DATE=datetime.datetime(2022, 1, 8, 0, 0))]
  
  assert sorted(get_CTE_VISITS_DISTINCT(encounters).collect()) == sorted(expected_output)

# COMMAND ----------

def test_get_OP_VISITS(encounters):
  CTE_VISITS_DISTINCT = get_CTE_VISITS_DISTINCT(encounters)
  
  expected_output=[
    Row(encounter_id='14', patient='3', encounterclass='ambulatory', VISIT_START_DATE=datetime.datetime(2022, 1, 2, 0, 0), VISIT_END_DATE=datetime.datetime(2022, 1, 3, 0, 0)),
    Row(encounter_id='17', patient='3', encounterclass='outpatient', VISIT_START_DATE=datetime.datetime(2022, 1, 3, 0, 0), VISIT_END_DATE=datetime.datetime(2022, 1, 5, 0, 0)),
    Row(encounter_id='16', patient='3', encounterclass='wellness', VISIT_START_DATE=datetime.datetime(2022, 1, 3, 0, 0), VISIT_END_DATE=datetime.datetime(2022, 1, 4, 0, 0)),
    Row(encounter_id='18', patient='4', encounterclass='ambulatory', VISIT_START_DATE=datetime.datetime(2022, 1, 5, 0, 0), VISIT_END_DATE=datetime.datetime(2022, 1, 6, 0, 0)),
    Row(encounter_id='19', patient='4', encounterclass='ambulatory', VISIT_START_DATE=datetime.datetime(2022, 1, 6, 0, 0), VISIT_END_DATE=datetime.datetime(2022, 1, 7, 0, 0)),
    Row(encounter_id='20', patient='4', encounterclass='outpatient', VISIT_START_DATE=datetime.datetime(2022, 1, 7, 0, 0), VISIT_END_DATE=datetime.datetime(2022, 1, 8, 0, 0))]
  
  assert sorted(get_OP_VISITS(CTE_VISITS_DISTINCT).collect()) == sorted(expected_output)

# COMMAND ----------

test_get_OP_VISITS(encounters())

# COMMAND ----------


