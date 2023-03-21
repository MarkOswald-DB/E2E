-- Databricks notebook source
-- TODO parameterize bronze layer

-- COMMAND ----------

CREATE LIVE TABLE ASSIGN_ALL_VISIT_IDS AS
SELECT
  E.id AS encounter_id,
  E.patient as person_source_value,
  E.start AS date_service,
  E.stop AS date_service_end,
  E.encounterclass,
  AV.encounterclass AS VISIT_TYPE,
  AV.VISIT_START_DATE,
  AV.VISIT_END_DATE,
  AV.VISIT_OCCURRENCE_ID,
  CASE
    WHEN E.encounterclass = 'inpatient'
    and AV.encounterclass = 'inpatient' THEN VISIT_OCCURRENCE_ID
    WHEN E.encounterclass in ('emergency', 'urgent') THEN (
      CASE
        WHEN AV.encounterclass = 'inpatient'
        AND E.start > AV.VISIT_START_DATE THEN VISIT_OCCURRENCE_ID
        WHEN AV.encounterclass in ('emergency', 'urgent')
        AND E.start = AV.VISIT_START_DATE THEN VISIT_OCCURRENCE_ID
        ELSE NULL
      END
    )
    WHEN E.encounterclass in ('ambulatory', 'wellness', 'outpatient') THEN (
      CASE
        WHEN AV.encounterclass = 'inpatient'
        AND E.start >= AV.VISIT_START_DATE THEN VISIT_OCCURRENCE_ID
        WHEN AV.encounterclass in ('ambulatory', 'wellness', 'outpatient') THEN VISIT_OCCURRENCE_ID
        ELSE NULL
      END
    )
    ELSE NULL
  END AS VISIT_OCCURRENCE_ID_NEW
FROM
  LIVE.ENCOUNTERS E
  INNER JOIN LIVE.ALL_VISITS AV ON E.patient = AV.patient
  AND E.start >= AV.VISIT_START_DATE
  AND E.start <= AV.VISIT_END_DATE;

-- COMMAND ----------

CREATE LIVE TABLE final_visit_ids AS
SELECT encounter_id, VISIT_OCCURRENCE_ID_NEW
FROM(
	SELECT *, ROW_NUMBER () OVER (PARTITION BY encounter_id ORDER BY PRIORITY) AS RN
	FROM (
		SELECT *,
			CASE
				WHEN encounterclass in ('emergency','urgent')
					THEN (
						CASE
							WHEN VISIT_TYPE = 'inpatient' AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
								THEN 1
							WHEN VISIT_TYPE in ('emergency','urgent') AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
								THEN 2
							ELSE 99
						END
					)
				WHEN encounterclass in ('ambulatory', 'wellness', 'outpatient')
					THEN (
						CASE
							WHEN VISIT_TYPE = 'inpatient' AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
								THEN  1
							WHEN VISIT_TYPE in ('ambulatory', 'wellness', 'outpatient') AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
								THEN 2
							ELSE 99
						END
					)
				WHEN encounterclass = 'inpatient' AND VISIT_TYPE = 'inpatient' AND VISIT_OCCURRENCE_ID_NEW IS NOT NULL
					THEN 1
				ELSE 99
			END AS PRIORITY
	FROM LIVE.ASSIGN_ALL_VISIT_IDS
	) T1
) T2
WHERE RN=1;

-- COMMAND ----------

CREATE LIVE TABLE AS 
SELECT
  ROW_NUMBER() OVER(
    ORDER BY
      p.id
  ),
  case
    upper(p.gender)
    when 'M' then 8507
    when 'F' then 8532
  end,
  YEAR(p.birthdate),
  MONTH(p.birthdate),
  DAY(p.birthdate),
  p.birthdate,
  case
    upper(p.race)
    when 'WHITE' then 8527
    when 'BLACK' then 8516
    when 'ASIAN' then 8515
    else 0
  end,
  case
    when upper(p.race) = 'HISPANIC' then 38003563
    else 0
  end,
  NULL,
  0,
  NULL,
  p.id,
  p.gender,
  0,
  p.race,
  0,
  p.ethnicity,
  0
from
  LIVE.patients p
where
  p.gender is not null;


-- COMMAND ----------

Create Live Table observation_period as
SELECT
  ROW_NUMBER() OVER(
    ORDER BY
      person_id
  ),
  person_id,
  start_date,
  end_date,
  44814724 AS period_type_concept_id
FROM
  (
    SELECT
      p.person_id,
      MIN(e.start) AS start_date,
      MAX(e.stop) AS end_date
    FROM
      LIVE.person p
      INNER JOIN LIVE.encounters e ON p.person_source_value = e.patient
    GROUP BY
      p.person_id
  ) tmp;

-- COMMAND ----------

Create Live Table visit_occurrence as
select
  av.visit_occurrence_id,
  p.person_id,
  case
    lower(av.encounterclass)
    when 'ambulatory' then 9202
    when 'emergency' then 9203
    when 'inpatient' then 9201
    when 'wellness' then 9202
    when 'urgentcare' then 9203
    when 'outpatient' then 9202
    else 0
  end,
  av.visit_start_date,
  av.visit_start_date,
  av.visit_end_date,
  av.visit_end_date,
  44818517,
  0,
  null,
  av.encounter_id,
  0,
  0,
  NULL,
  0,
  NULL,
  lag(visit_occurrence_id) over(
    partition by p.person_id
    order by
      av.visit_start_date
  )
from
  LIVE.all_visits av
  join LIVE.person p on av.patient = p.person_source_value
where
  av.visit_occurrence_id in (
    select
      distinct visit_occurrence_id_new
    from
      final_visit_ids
  );

