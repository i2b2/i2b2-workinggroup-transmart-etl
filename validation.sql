
-----
---ETL
------
 
--RACE
select  race_cd, count(*) group_cnt,
sum(count(*)) over() total_cnt,
          round(100*(count(*) / sum(count(*)) over ()),2) perc
from patient_dimension group by race_cd
order by 4 desc
 
--SEX
select  sex_cd, count(*) group_cnt,
sum(count(*)) over() total_cnt,
          round(100*(count(*) / sum(count(*)) over ()),2) perc
from patient_dimension group by sex_cd
order by 4 desc
 
 
--VITAL_STATUS
select  vital_status_cd, count(*) from patient_dimension group by vital_status_cd
 
--AGE (sql Server)
select
   case when age <= 0 then 'Under 1'
       when age < 1 and age > 0 then '0-1'
       when age < 5 and age > 1 then '2-4'
       when age < 10 and age > 4 then '5-9'
       when age < 15 and age > 10 then '10-14'
       when age < 19 and age > 14 then '15-18'
       when age < 22 and age > 18 then '19-21'
       when age < 45 and age > 21 then '22-44'
       when age < 65 and age > 44 then '45-64'
       when age < 75 and age > 64 then '65-74'
       when age < 90 and age > 74 then '75-89'
       when age > 89 then  'Over 89' 
else 'unkn' end as range,
  count(*) as count
from (select DATEDIFF(yy, birth_date, GETDATE()) as age from patient_dimension) c
group by   case when age <= 0 then 'Under 1'
       when age < 1 and age > 0 then '0-1'
       when age < 5 and age > 1 then '2-4'
       when age < 10 and age > 4 then '5-9'
       when age < 15 and age > 10 then '10-14'
       when age < 19 and age > 14 then '15-18'
       when age < 22 and age > 18 then '19-21'
       when age < 45 and age > 21 then '22-44'
       when age < 65 and age > 44 then '45-64'
       when age < 75 and age > 64 then '65-74'
       when age < 90 and age > 74 then '75-89'
       when age > 89 then  'Over 89' 
   else  'unkn' end
 
--AGE (Oracle)
select
  case when age <= 0 then 'Under 1'
       when age < 1 and age > 0 then '0-1'
       when age < 5 and age > 1 then '2-4'
       when age < 10 and age > 4 then '5-9'
       when age < 15 and age > 10 then '10-14'
       when age < 19 and age > 14 then '15-18'
       when age < 22 and age > 18 then '19-21'
       when age < 45 and age > 21 then '22-44'
       when age < 65 and age > 44 then '45-64'
       when age < 75 and age > 64 then '65-74'
       when age < 90 and age > 74 then '75-89'
       when age > 89 then  'Over 89' 
else 'unkn' end as range,
  count(*) as count
from (select MONTHS_BETWEEN(SYSTIMESTAMP, birth_date) / 12 as age from patient_dimension) c
group by   case when age <= 0 then 'Under 1'
       when age < 1 and age > 0 then '0-1'
       when age < 5 and age > 1 then '2-4'
       when age < 10 and age > 4 then '5-9'
       when age < 15 and age > 10 then '10-14'
       when age < 19 and age > 14 then '15-18'
       when age < 22 and age > 18 then '19-21'
       when age < 45 and age > 21 then '22-44'
       when age < 65 and age > 44 then '45-64'
       when age < 75 and age > 64 then '65-74'
       when age < 90 and age > 74 then '75-89'
       when age > 89 then  'Over 89' 
   else  'unkn' end
order by range
 
-- Encounter Type
select  inout_cd, count(*) group_cnt,
sum(count(*)) over() total_cnt,
          round(100*(count(*) / sum(count(*)) over ()),2) perc
from visit_dimension group by inout_cd
order by 4 desc
 
-- Length of Stay
select  length_of_stay, count(*) group_cnt,
sum(count(*)) over() total_cnt,
          round(100*(count(*) / sum(count(*)) over ()),2) perc
from visit_dimension group by length_of_stay
order by 1 
 
-- Group by year encounters
SELECT  EXTRACT(year FROM start_date) "Year", count(*) group_cnt,
sum(count(*)) over() total_cnt, round(100*(count(*) / sum(count(*)) over ()),2) perc
FROM visit_dimension
GROUP BY EXTRACT(year FROM start_date)
ORDER BY EXTRACT(year FROM start_date)
 
-- Group by year encounters discharge
SELECT  EXTRACT(year FROM start_date) "Year", count(*) group_cnt,
sum(count(*)) over() total_cnt, round(100*(count(*) / sum(count(*)) over ()),2) perc
FROM visit_dimension
where start_date != end_date
GROUP BY EXTRACT(year FROM start_date)
ORDER BY EXTRACT(year FROM start_date)
 
 
-- Group by year observation_fact
SELECT  EXTRACT(year FROM start_date) "Year", count(*) group_cnt,
sum(count(*)) over() total_cnt, round(100*(count(*) / sum(count(*)) over ()),2) perc
FROM observation_fact
GROUP BY EXTRACT(year FROM start_date)
ORDER BY EXTRACT(year FROM start_date)
 
-- Group by year observation_fact discharge
SELECT  EXTRACT(year FROM end_date) "Year", count(*) group_cnt,
sum(count(*)) over() total_cnt, round(100*(count(*) / sum(count(*)) over ()),2) perc
FROM observation_fact
where  end_date is not null
GROUP BY EXTRACT(year FROM end_date)
ORDER BY EXTRACT(year FROM end_date)
 
 
--Number of patient in Patient Dimension but not in observation_fact
SELECT  COUNT( *)
FROM    patient_dimension
WHERE   patient_num NOT IN (SELECT  distinct patient_num FROM observation_fact)
 
--Number of patient in  observation_fact but not in  Patient Dimension 
SELECT  COUNT(*)
FROM    observation_fact
WHERE   patient_num NOT IN (SELECT distinct patient_num FROM patient_dimension)
 
--Concepts in observation fact not in concept dimension
SELECT  distinct concept_cd 
FROM    observation_fact
WHERE   concept_cd NOT IN (SELECT distinct concept_cd FROM concept_dimension)
 
 
 
-------
-- Stats
--------
--Number of queries in last year
SELECT TO_CHAR(TO_DATE(EXTRACT(month FROM create_date), 'MM'), 'MONTH') "Month", count(*)
FROM qt_query_master
WHERE create_date >= trunc(sysdate, 'yyyy') - interval '1' year
GROUP BY EXTRACT(month FROM create_date)
ORDER BY EXTRACT(month FROM create_date)
 
--users in the last year
SELECT user_id, count(*)
FROM qt_query_master
WHERE create_date >= trunc(sysdate, 'yyyy') - interval '1' year
GROUP BY user_id
ORDER BY user_id
 
------
-- PM
-----
-- Breakdown of admin calls
select attempt_cd, count(*)
from pm_user_login
where attempt_cd != upper(attempt_cd)
group by attempt_cd
 
-- Breakdown of user calls
select attempt_cd, count(*)
from pm_user_login
where attempt_cd != lower(attempt_cd)
group by attempt_cd
 
-- Breakdown of roles
select project_id, user_role_cd, count(*)
from pm_project_user_roles
group by project_id, user_role_cd
order by project_id, user_role_cd
 
 
 
 
From: "Mendis, Michael E." <MMENDIS@PARTNERS.ORG>
Date: Tuesday, October 2, 2018 at 4:09 PM
To: "etl-working-group@googlegroups.com" <etl-working-group@googlegroups.com>
Subject: Re: ETL Working Group Meeting
 
First pass at some SQL, what others would we like to have?
 
-----
---ETL
------
 
--RACE
select  race_cd, count(*) from patient_dimension group by race_cd
 
--SEX
select  sex_cd, count(*) from patient_dimension group by sex_cd
 
--VITAL
select  vital_status_cd, count(*) from patient_dimension group by vital_status_cd
 
--AGE (sql Server)
select
  case when age < 10 then 'Under 10'
       when age < 21 and age > 10 then '11-20'
       when age < 31 and age > 20 then '21-30'
       when age < 41 and age > 30 then '31-40'
       when age < 51 and age > 40 then '41-50'
       when age < 61 and age > 50 then '51-60'
       when age < 71 and age > 60 then '61-70'
       when age < 81 and age > 70 then '71-80'
       when age < 86 and age > 80 then '85-80'
       when age > 85 then  'Over 85' 
else 'unkn' end as range,
  count(*) as count
from (select DATEDIFF(yy, birth_date, GETDATE()) as age from patient_dimension) c
group by case when age < 10 then 'Under 10'
       when age < 21 and age > 10 then '11-20'
       when age < 31 and age > 20 then '21-30'
       when age < 41 and age > 30 then '31-40'
       when age < 51 and age > 40 then '41-50'
       when age < 61 and age > 50 then '51-60'
       when age < 71 and age > 60 then '61-70'
       when age < 81 and age > 70 then '71-80'
       when age < 86 and age > 80 then '85-80'
  when age > 85 then  'Over 85'
   else  'unkn' end
 
--AGE (Oracle)
select
  case when age < 10 then 'Under 10'
       when age < 21 and age > 10 then '11-20'
       when age < 31 and age > 20 then '21-30'
       when age < 41 and age > 30 then '31-40'
       when age < 51 and age > 40 then '41-50'
       when age < 61 and age > 50 then '51-60'
       when age < 71 and age > 60 then '61-70'
       when age < 81 and age > 70 then '71-80'
       when age < 86 and age > 80 then '85-80'
       when age > 85 then  'Over 85' 
else 'unkn' end as range,
  count(*) as count
from (select MONTHS_BETWEEN(SYSTIMESTAMP, birth_date) / 12 as age from patient_dimension) c
group by case when age < 10 then 'Under 10'
       when age < 21 and age > 10 then '11-20'
       when age < 31 and age > 20 then '21-30'
       when age < 41 and age > 30 then '31-40'
       when age < 51 and age > 40 then '41-50'
       when age < 61 and age > 50 then '51-60'
       when age < 71 and age > 60 then '61-70'
       when age < 81 and age > 70 then '71-80'
       when age < 86 and age > 80 then '85-80'
  when age > 85 then  'Over 85'
   else  'unkn' end
order by range
 
 
--Number of patient in Patient Dimension but not in observation_fact
SELECT  COUNT( *)
FROM    patient_dimension
WHERE   patient_num NOT IN (SELECT  distinct patient_num FROM observation_fact)
 
--Number of patient in  observation_fact but not in  Patient Dimension 
SELECT  COUNT(*)
FROM    observation_fact
WHERE   patient_num NOT IN (SELECT distinct patient_num FROM patient_dimension)
 
--Concepts in observation fact not in concept dimension
SELECT  distinct concept_cd 
FROM    observation_fact
WHERE   concept_cd NOT IN (SELECT distinct concept_cd FROM concept_dimension)
 
 
 
-------
-- Stats
--------
--Number of queries in last year
SELECT TO_CHAR(TO_DATE(EXTRACT(month FROM create_date), 'MM'), 'MONTH') "Month", count(*)
FROM qt_query_master
WHERE create_date >= trunc(sysdate, 'yyyy') - interval '1' year
GROUP BY EXTRACT(month FROM create_date)
ORDER BY EXTRACT(month FROM create_date)
 
--users in the last year
SELECT user_id, count(*)
FROM qt_query_master
WHERE create_date >= trunc(sysdate, 'yyyy') - interval '1' year
GROUP BY user_id
ORDER BY user_id
 
------
-- PM
-----
-- Breakdown of admin calls
select attempt_cd, count(*)
from pm_user_login
where attempt_cd != upper(attempt_cd)
group by attempt_cd
 
-- Breakdown of user calls
select attempt_cd, count(*)
from pm_user_login
where attempt_cd != lower(attempt_cd)
group by attempt_cd
 
-- Breakdown of roles
select project_id, user_role_cd, count(*)
from pm_project_user_roles
group by project_id, user_role_cd
order by project_id, user_role_cd
 
