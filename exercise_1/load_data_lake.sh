#!/bin/bash

tail -n +2 Hospital\ General\ Information.csv > hospitals.csv
tail -n +2 Timely\ and\ Effective\ Care\ -\ Hospital.csv > effective_care.csv
tail -n +2 Readmissions\ and\ Deaths\ -\ Hospital.csv > readmissions.csv
tail -n +2 Measure\ Dates.csv > measures.csv
tail -n +2 hvbp_hcahps_05_28_2015.csv > surveys_responses.csv

hdfs dfs -mkdir /user/w205/hospital_compare
hdfs dfs -mkdir /user/w205/hospital_compare/hospitals
hdfs dfs -mkdir /user/w205/hospital_compare/effective_care
hdfs dfs -mkdir /user/w205/hospital_compare/readmissions
hdfs dfs -mkdir /user/w205/hospital_compare/measures
hdfs dfs -mkdir /user/w205/hospital_compare/surveys_responses


hdfs dfs -put hospitals.csv /user/w205/hospital_compare/hospitals
hdfs dfs -put effective_care.csv /user/w205/hospital_compare/effective_care
hdfs dfs -put readmissions.csv /user/w205/hospital_compare/readmissions
hdfs dfs -put measures.csv /user/w205/hospital_compare/measures
hdfs dfs -put surveys_responses.csv /user/w205/hospital_compare/surveys_responses
