DROP TABLE hospitals_tbl;
DROP TABLE effective_care_tbl;
DROP TABLE readmissions_tbl;
DROP TABLE measures_tbl;
DROP TABLE surveys_responses_tbl;

DROP TABLE tbl_surveys_responses;
DROP TABLE tbl_hospitals;
DROP TABLE tbl_measures;
DROP TABLE tbl_effective_care;
DROP TABLE tbl_readmissions;

CREATE EXTERNAL TABLE hospitals_tbl(provider_id STRING, hospital_name STRING ,address STRING, city STRING , state STRING , zip STRING ,country STRING , ph_no STRING ,hospital_type STRING , ownership STRING , emergency_services STRING )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/hospitals';


CREATE EXTERNAL TABLE effective_care_tbl(provider_id STRING, hospital_name STRING ,
	address STRING, city STRING , state STRING , zip STRING ,country STRING , 
	ph_no STRING ,Condition STRING ,measure_id STRING, measure_name STRING, 
	score STRING, sample STRING, footnote STRING,measure_start_date DATE, measure_end_date DATE )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/effective_care';

CREATE EXTERNAL TABLE readmissions_tbl(provider_id STRING, hospital_name STRING ,
	address STRING, city STRING , state STRING , zip STRING ,
	country STRING , ph_no STRING , measure_name STRING, measure_id STRING, 
	compared_to_national STRING, denominator STRING, score INT, 
	lower_estimate STRING, higher_estimate STRING, footnote STRING,measure_start_date STRING, 
	measure_end_date STRING )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/readmissions';

CREATE EXTERNAL TABLE measures_tbl(measure_name STRING, measure_id STRING, 
	start_quarter STRING, start_date DATE, end_quarter STRING, end_date DATE )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/measures';

CREATE EXTERNAL TABLE surveys_responses_tbl(provider_id STRING, hospital_name STRING ,
	address STRING, city STRING , state STRING , zip STRING ,
	country STRING ,cwn_achivement STRING , cwn_improvement STRING ,
	cwn_dimension STRING, cwd_achievement STRING, cwd_improvement STRING,
	 cwd_dimension STRING, rhs_achievement STRING, rhs_improvement STRING,
	  rhs_dimension STRING, pm_achievement STRING, pm_improvement STRING, 
	  pm_dimension STRING, cm_achievement STRING, cm_improvement STRING , 
	  cm_dimension STRING, cqhe_achievement STRING, cqhe_improvement STRING, 
	  cqhe_dimension STRING, di_achievement STRING, di_improvement STRING,
	   di_dimension STRING, orh_achievement STRING, orh_improvement STRING, 
	   orh_dimension STRING, hcahps_base_score STRING, hcahps_consistency_score STRING  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
"separatorChar" = ",",
"quoteChar" = '"',
"escapeChar" = '\\'
)
STORED AS TEXTFILE
LOCATION '/user/w205/hospital_compare/surveys_responses';

CREATE TABLE tbl_surveys_responses
AS SELECT provider_id,
CAST(SPLIT(cwn_achivement,' ')[0] AS INT) AS cwn_achivement,
CAST(SPLIT(cwn_improvement,' ')[0] AS INT) AS cwn_improvement,
CAST(SPLIT(cwn_dimension,' ')[0] AS INT) AS cwn_dimension,
CAST(SPLIT(cwd_achievement,' ')[0] AS INT) AS cwd_achievement,
CAST(SPLIT(cwd_improvement,' ')[0] AS INT) AS cwd_improvement,
CAST(SPLIT(cwd_dimension,' ')[0] AS INT) AS cwd_dimension,
CAST(SPLIT(rhs_achievement,' ')[0] AS INT) AS rhs_achievement,
CAST(SPLIT(rhs_improvement,' ')[0] AS INT) AS rhs_improvement,
CAST(SPLIT(rhs_dimension,' ')[0] AS INT) AS rhs_dimension,
CAST(SPLIT(pm_achievement,' ')[0] AS INT) AS pm_achievement,
CAST(SPLIT(pm_improvement,' ')[0] AS INT) AS pm_improvement,
CAST(SPLIT(pm_dimension,' ')[0] AS INT) AS pm_dimension,
CAST(SPLIT(cm_achievement,' ')[0] AS INT) AS cm_achievement,
CAST(SPLIT(cm_improvement,' ')[0] AS INT) AS cm_improvement,
CAST(SPLIT(cm_dimension,' ')[0] AS INT) AS cm_dimension,
CAST(SPLIT(cqhe_achievement,' ')[0] AS INT) AS cqhe_achievement,
CAST(SPLIT(cqhe_improvement,' ')[0] AS INT) AS cqhe_improvement,
CAST(SPLIT(cqhe_dimension,' ')[0] AS INT) AS cqhe_dimension,
CAST(SPLIT(di_achievement,' ')[0] AS INT) AS di_achievement,
CAST(SPLIT(di_improvement,' ')[0] AS INT) AS di_improvement,
CAST(SPLIT(di_dimension,' ')[0] AS INT) AS di_dimension,
CAST(SPLIT(orh_achievement,' ')[0] AS INT) AS orh_achievement,
CAST(SPLIT(orh_improvement,' ')[0] AS INT) AS orh_improvement,
CAST(SPLIT(orh_dimension,' ')[0] AS INT) AS orh_dimension,
CAST(SPLIT(hcahps_base_score,' ')[0] AS INT) AS hcahps_base_score,
CAST(SPLIT(hcahps_consistency_score,' ')[0] AS INT) AS hcahps_consistency_score
FROM surveys_responses_tbl
where lower(cwn_achivement) <> "not available" and  lower(cwn_improvement) <> "not available" and 
lower(cwn_dimension) <> "not available" and lower(cwd_achievement) <> "not available" and 
lower(cwd_improvement) <> "not available" and 	 lower(cwd_dimension) <> "not available" and 
lower(rhs_achievement) <> "not available" and lower(rhs_improvement) <> "not available" and 	  
lower(rhs_dimension) <> "not available" and lower(pm_achievement) <> "not available" and 
lower(pm_improvement) <> "not available" and lower(pm_dimension) <> "not available" and 
lower(cm_achievement) <> "not available" and lower(cm_improvement) <> "not available" and  
lower(cm_dimension) <> "not available" and lower(cqhe_achievement) <> "not available" and 
lower(cqhe_improvement) <> "not available" and lower(cqhe_dimension) <> "not available" and 
lower(di_achievement) <> "not available" and lower(di_improvement) <> "not available" and 	   
lower(di_dimension) <> "not available" and lower(orh_achievement) <> "not available" and 
lower(orh_improvement) <> "not available" and lower(orh_dimension) <> "not available" and 
lower(hcahps_base_score) <> "not available" and lower(hcahps_consistency_score) <> "not available" ;


CREATE TABLE tbl_hospitals 
AS SELECT lower(provider_id) as provider_id, hospital_name, address, lower(city) as city, lower(state) as state 
, zip , lower(country) as country, ph_no, 
lower(hospital_type) as hospital_type,
lower(ownership)  AS ownership, 
CASE
	WHEN lower(emergency_services) == "yes" THEN 1
	WHEN lower(emergency_services) == "no" THEN 0
	ELSE 99
END AS emergency_services 
FROM hospitals_tbl ;


CREATE TABLE tbl_measures 
AS SELECT 
lower(measure_id) AS measure_id,
lower(measure_name) AS measure_name, 
start_date,
end_date,
start_quarter,
end_quarter
FROM measures_tbl;


CREATE TABLE tbl_effective_care
AS SELECT
lower(provider_id) AS provider_id,
lower(measure_id) AS measure_id,
lower(condition) condition_id,
CASE
	WHEN lower(score) LIKE 'high%' THEN 20
	WHEN lower(score) LIKE 'medium%' THEN 60
	WHEN lower(score) LIKE 'low%' THEN 100
	WHEN lower(score) LIKE 'very high%' THEN 0
	WHEN lower(score) == "not available" THEN -1
	WHEN (lower(measure_id) == "op_5" or lower(measure_id) == "op_3b" or 
		lower(measure_id) == "ed_1b" or lower(measure_id) == "ed_2b" or 
		lower(measure_id) == "op_18b" or lower(measure_id) == "op_20" or 
		lower(measure_id) == "op_21") THEN IF(score <100 ,100 ,IF(score <300,80 , IF(score <600 ,60 , IF(score <800 ,40 , IF(score <1000 ,20 , IF(score >1000 ,5 , 0))))) )
	ELSE cast(score as DOUBLE)
END AS score,
CASE
	WHEN lower(sample) == "not available" THEN -1
	ELSE cast(sample as INT)
END AS sample,
lower(footnote) AS footnote
FROM effective_care_tbl;


CREATE TABLE tbl_readmissions 
AS SELECT
lower(provider_id) AS provider_id,
lower(measure_id) AS measure_id,
CASE
	WHEN lower(compared_to_national) == "better than the national rate" THEN 10
	WHEN lower(compared_to_national) == "no different than the national rate" THEN 5 
	WHEN lower(compared_to_national) == "worse than the national rate" THEN 1
	WHEN lower(compared_to_national) == "number of cases too small" THEN -1
	WHEN lower(compared_to_national) == "not available" THEN -1
	ELSE -1
END AS compared_to_national,
CASE
	WHEN lower(denominator) == "not available" THEN -1
	ELSE cast(denominator as INT)
END AS denominator,
CASE
	WHEN lower(score) == "not available" THEN -1
	ELSE cast(score as DOUBLE)
END AS score,
CASE
	WHEN lower(lower_estimate) == "not available" THEN -1
	ELSE cast(lower_estimate as DOUBLE)
END AS lower_estimate,
CASE
	WHEN lower(higher_estimate) == "not available" THEN -1
	ELSE cast(higher_estimate as DOUBLE)
END AS higher_estimate
FROM readmissions_tbl;

