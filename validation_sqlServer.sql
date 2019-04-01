
/*
 * Create a table in the crcdata database that will hold the results
 * of analysis after ETL is complete
 */

-- The ETL_RESULTS table holds information that can be obtained from
-- scanning the OBSERVATION_FACT and associated dimension tables for
-- information.

IF OBJECT_ID('dbo.ETL_RESULTS', 'U') IS NOT NULL
  DROP TABLE dbo.ETL_RESULTS;

CREATE TABLE dbo.ETL_RESULTS (
    analysis_id INT,
    status_cd VARCHAR(10),
	i2b2_warning VARCHAR(MAX),
	date_of_testing DATETIME
);

IF OBJECT_ID('dbo.ETL_COMPLETENESS_ANALYSIS', 'U') IS NOT NULL
  DROP TABLE dbo.ETL_COMPLETENESS_ANALYSIS;

CREATE TABLE dbo.ETL_COMPLETENESS_ANALYSIS (
    analysis_id INT,
    [TABLE_CD] VARCHAR(20),
	[DESCRIPTION] VARCHAR(MAX),
	[NUM_RECORDS] VARCHAR(MAX),
	[PERCENTAGE] DECIMAL(18, 2),
	date_of_testing DATETIME
);

IF OBJECT_ID('dbo.ETL_PROV_DENSITY_ANALYSIS', 'U') IS NOT NULL
  DROP TABLE dbo.ETL_PROV_DENSITY_ANALYSIS;

CREATE TABLE dbo.ETL_PROV_DENSITY_ANALYSIS (
    analysis_id INT,
	[PROVIDER_ID] VARCHAR(MAX),
	[NUM_RECORDS] VARCHAR(MAX),
	[PERCENTAGE] DECIMAL(18, 2),
	date_of_testing DATETIME
);

/*
 * The following checks were adapted from ACHILLES Heel. Not all of
 * the checks in ACHILLES are done for i2b2 due to the differences
 * between the two domains. However, the general spirit of the checks
 * remain the same.
 */

-- Rule 1: Look for BIRTH_DATEs in the future in PATIENT_DIMENSION
insert into ETL_RESULTS (analysis_id, status_cd, i2b2_warning, date_of_testing)
select 1 as analysis_id
     ,'ERROR'
	 , CONCAT('Patient ', CAST(PATIENT_NUM AS VARCHAR), ' from ', cast(sourcesystem_cd as varchar)
		, ' has a birth date after the current day this test was run. Today''s date: ', getDate(), ' Birth date:', BIRTH_DATE)  
	 , getDate()
from dbo.patient_dimension
where BIRTH_DATE > GETDATE();

-- Rule 2: Look for cases where DEATH_DATE < BIRTH_DATE in PATIENT_DIMENSION
insert into ETL_RESULTS (analysis_id, status_cd, i2b2_warning, date_of_testing)
select 2 as analysis_id
     ,'ERROR'
	 , CONCAT('Patient ', CAST(PATIENT_NUM AS VARCHAR), ' from ', cast(sourcesystem_cd as varchar)
		, ' has a death date before their birth_date. Death date: ', death_date, ' Birth date:', BIRTH_DATE)  
	 , getDate()
from dbo.patient_dimension
where DEATH_DATE is not null
	and DEATH_DATE < BIRTH_DATE;

-- Rule 3: Look for BIRTH_DATEs prior to 1800 in PATIENT_DIMENSION
insert into ETL_RESULTS (analysis_id, status_cd, i2b2_warning, date_of_testing)
select 3 as analysis_id
     ,'ERROR'
	 , CONCAT('Patient ', CAST(PATIENT_NUM AS VARCHAR), ' from ', cast(sourcesystem_cd as varchar), ' has a birth date prior to 1800. Birth date:', BIRTH_DATE)  
	 , getDate()
from dbo.patient_dimension
where YEAR(BIRTH_DATE) <= 1800;

-- Rule 4: Look for AGE < 0
insert into ETL_RESULTS (analysis_id, status_cd, i2b2_warning, date_of_testing)
select 4 as analysis_id
     ,'ERROR'
	 , CONCAT('Patient ', CAST(PATIENT_NUM AS VARCHAR), ' from ', cast(sourcesystem_cd as varchar), ' has a negative age. Age:', AGE_IN_YEARS_NUM)  
	 , getDate()
from dbo.patient_dimension
where AGE_IN_YEARS_NUM < 0;

-- Rule 5: Look for AGE >= 150
insert into ETL_RESULTS (analysis_id, status_cd, i2b2_warning, date_of_testing)
select 5 as analysis_id
     ,'ERROR'
	 , CONCAT('Patient ', CAST(PATIENT_NUM AS VARCHAR), ' from ', cast(sourcesystem_cd as varchar), ' has an age of over 150. Age:', AGE_IN_YEARS_NUM)  
	 , getDate()
from dbo.patient_dimension
where AGE_IN_YEARS_NUM >= 150;

-- Rule 6: Look for concept codes in OBSERVATION_FACT which are not present in CONCEPT_DIMENSION
insert into ETL_RESULTS (analysis_id, status_cd, i2b2_warning, date_of_testing)
select 6 as analysis_id
     ,'WARNING'
	 , CONCAT('The following concept in the fact table is not in the concept dimension table: ', concept_cd)
	 , getDate()
from (
	select distinct o.concept_cd from observation_fact [o]
	left join concept_dimension d on o.concept_cd = d.concept_cd
	where d.concept_cd is null
) [rule 6];

-- Rule 7: Look for modifier codes in OBSERVATION_FACT which are not present in MODIFIER_DIMENSION
insert into ETL_RESULTS (analysis_id, status_cd, i2b2_warning, date_of_testing)
select 7 as analysis_id
     ,'WARNING'
	 , CONCAT('The following modifiers in the fact table is not in the modifier dimension table: ', modifier_cd)
	 , getDate()
from (
	select distinct o.modifier_cd from observation_fact [o]
	left join modifier_dimension d on o.modifier_cd = d.modifier_cd
	where d.modifier_cd is null
) [rule 7];

/*
-- Rule 8: Look for source system codes in OBSERVATION_FACT which are not present in SOURCE_MASTER
insert into ETL_RESULTS (analysis_id, status_cd, i2b2_warning, date_of_testing)
select 8 as analysis_id
	 , CONCAT('WARNING: The following source systems in the fact table is not in the source master table: ', sourcesystem_cd)
	 , getDate()
from (
	select distinct o.sourcesystem_cd from observation_fact [o]
	left join source_master d on o.sourcesystem_cd = d.sourcesystem_cd
	where d.sourcesystem_cd is null
) [rule 8];
*/


-- Rule 9: Look for visits that end before they start in VISIT_DIMENSION
insert into ETL_RESULTS (analysis_id, status_cd, i2b2_warning, date_of_testing)
select 9 as analysis_id
     ,'ERROR'
	 , CONCAT('Patient ', CAST(PATIENT_NUM AS VARCHAR), ' from ', cast(sourcesystem_cd as varchar)
		, ' has a visit that ends before it starts. End date: ', end_date, ' Start date:', [START_DATE])  
	 , getDate()
from dbo.visit_dimension
where end_date is not null 
	and [end_date] > [start_date];

	/*
-- Rule 10: Look for source systems of visits in VISIT_DIMENSION which are not present in SOURCE_MASTER
insert into ETL_RESULTS (analysis_id, status_cd, i2b2_warning, date_of_testing)
select 10 as analysis_id
	 , CONCAT('WARNING: The following source systems in the VISIT_DIMENSION table is not in the source master table: ', sourcesystem_cd)
	 , getDate()
from (
	select distinct o.sourcesystem_cd from dbo.visit_dimension [o]
	left join source_master d on o.sourcesystem_cd = d.sourcesystem_cd
	where d.sourcesystem_cd is null
) [rule 10];	
*/

-- Rule 11: Look for encounters with no START_DATE
insert into ETL_RESULTS (analysis_id, status_cd, i2b2_warning, date_of_testing)
select 11 as analysis_id
     ,'ERROR'
	 , CONCAT('Patient ', CAST(PATIENT_NUM AS VARCHAR), ' from ', cast(sourcesystem_cd as varchar), ' has a visit with no start date.')  
	 , getDate()
from dbo.visit_dimension
where [start_date] is null;

-- Rule 12: Look for providers in the fact table that are not in PROVIDER_DIMENSION
-- This does not include visits that have no provider.
insert into ETL_RESULTS (analysis_id, status_cd, i2b2_warning, date_of_testing)
select 12 as analysis_id
     ,'WARNING'
	 , CONCAT('The following provider in the fact table is not in the provider dimension table: ', provider_id)
	 , getDate()
from (
	select distinct o.provider_id from dbo.observation_fact [o]
	left join dbo.provider_dimension d on o.provider_id = d.provider_id
	where d.provider_id is null and o.provider_id <> '@'
) [rule 12];

/*
-- Rule 13: Look for instances of an MRN being assigned to more than one i2b2 patient number
insert into ETL_RESULTS (analysis_id, status_cd, i2b2_warning, date_of_testing)
select  13 as analysis_id
	  , CONCAT('WARNING: The following MRNs are assigned more than one i2b2 patient number: ', lcl_id)
	 , getDate()
from (
	select lcl_id
		 , count(distinct global_id) [count]
	from heroni2b2imdata.dbo.im_mpi_Mapping
	group by lcl_Id
) [rule 13]
where [count] <> 1;
*/

-- Rule 14: Look for visit location values in the fact table that is not in VISIT_DIMENSION
-- Does not include results that have no location.
insert into ETL_RESULTS (analysis_id, status_cd, i2b2_warning, date_of_testing)
select 14 as analysis_id
     ,'WARNING'
	 , CONCAT('The following location code in the fact table is not in the visit dimension table: ', location_cd)
	 , getDate()
from (
	select o.location_cd from (select distinct location_cd from dbo.observation_fact where location_cd <> '@' ) [o]
	left join ( select distinct location_cd from dbo.visit_dimension ) d on o.location_cd = d.location_cd
	where d.location_cd is null
) [rule 14];

-- Rule 15: Look for patients that have observations after a recorded death date
insert into ETL_RESULTS (analysis_id, status_cd, i2b2_warning, date_of_testing)
select 15 as analysis_id
     ,'ERROR'
	 , CONCAT('Patient ', CAST(pd.PATIENT_NUM AS VARCHAR), ' from ', cast(o.sourcesystem_cd as varchar)
		, ' has an encounter after their death. Death date: ', death_date, ' Encounter date:', O.START_DATE, 'Encounter number: ', O.ENCOUNTER_NUM)  
	 , getDate()
from dbo.patient_dimension pd
join ( select distinct patient_num, encounter_num, start_date, sourcesystem_cd from dbo.OBSERVATION_FACT) o
	on pd.PATIENT_NUM = o.PATIENT_NUM
where DEATH_DATE is not null
	and o.START_DATE > DEATH_DATE;

-- Rule 16: Look for patients that have visits before their birth
insert into ETL_RESULTS (analysis_id, status_cd, i2b2_warning, date_of_testing)
select 16 as analysis_id
     ,'ERROR'
	 , CONCAT('Patient ', CAST(pd.PATIENT_NUM AS VARCHAR), ' from ', cast(o.sourcesystem_cd as varchar)
		, ' has an encounter before their birth. Birth date: ', birth_date, ' Encounter date:', O.START_DATE, 'Encounter number: ', O.ENCOUNTER_NUM)  
	 , getDate()
from dbo.patient_dimension pd
join (select distinct encounter_num, patient_num, start_date, sourcesystem_cd from dbo.OBSERVATION_FACT) o
	on pd.PATIENT_NUM = o.PATIENT_NUM
where o.START_DATE < BIRTH_DATE;

-- Rule 17: Metadata elements with missing C_PATH or C_SYMBOL
insert into ETL_RESULTS (analysis_id, status_cd, i2b2_warning, date_of_testing)
select 17 as analysis_id
     ,'WARNING'
	 , CONCAT('The metadata node ', c_fullname, ' has null C_PATH and/or C_SYMBOL values. C_PATH: ', coalesce(c_path, 'NULL'), ', C_SYMBOL: ', coalesce(c_symbol,'NULL'))  
	 , getDate()
from (
	select c_fullname, c_path, c_symbol from dbo.i2b2
	where c_path is null or c_symbol is null
	union
	select c_fullname, c_path, c_symbol from dbo.custom_meta
	where c_path is null or c_symbol is null
) [rule 17]

-- Rule 18: Metadata elements where the C_PATH + C_SYMBOL <> C_FULLNAME
insert into ETL_RESULTS (analysis_id, status_cd, i2b2_warning, date_of_testing)
select 18 as analysis_id
     ,'WARNING'
	 , CONCAT('The metadata node ', c_fullname, ' has incorrect C_PATH and/or C_SYMBOL values. C_PATH: ', c_path, ', C_SYMBOL: ', c_symbol)  
	 , getDate()
from (
	select c_fullname, c_path, c_symbol from dbo.i2b2
	where c_path is null or c_symbol is null
	union
	select c_fullname, c_path, c_symbol from dbo.custom_meta
	where c_path is null or c_symbol is null
) [rule 18]

-- Rule 19: Number of patient in  observation_fact but not in  Patient Dimension 
insert into ETL_RESULTS (analysis_id, status_cd, i2b2_warning, date_of_testing)
select 18 as analysis_id
     ,'ERROR'
	 , CONCAT('Patient ', patient_num, ' is in observation_fact but not in Patient Dimension')  
	 , getDate()
from (
	select patient_num
FROM    observation_fact
WHERE   patient_num NOT IN (SELECT distinct patient_num FROM patient_dimension)
) [rule 19]

/*
 * Begin analysis of data by running some SQL queries
 * and recording the results. STRATUM_1 and STRATUM_2
 * are columns to hold partially computed results from
 * these tests for later analysis. The percentage affected
 * icon holds the percentage of the data set that the
 * analysis covers
 *
 * Analysis ID 1 calculates a breakdown of facts per month. 
	STRATUM_1 holds the date. STRATUM_2 holds the patient count.
 * Analysis ID 2 calculates a breakdown of the ratio of providers to total patients. 
	STRATUM_1 holds the provider_id, STRATUM_2 holds the patient count
 * Analysis ID 3 calculates a percentage of patients with at least one measurement, diagnoses, and medication.
	STRATUM_1 describes this analysis.. STRATUM_2 has the patient count
 * Analysis ID 4 calculates the percentage of patients with no encounters
	STRATUM_1 holds the percentage of patients with no encounters. STRATUM_2 holds the total number of patients in the project.
 */
DECLARE @DENOMINATOR_PAT bigint = (SELECT COUNT(distinct patient_num) FROM dbo.PATIENT_DIMENSION)

-- Some observation facts, such as MRN, social determinants of health, and vital status, have negative encounter
-- numbers because of the fact that they do not capture information on the encounter level, but are easiest to
-- express, store, and retrieve from the fact table. We exclude these from our analysis.
insert into ETL_COMPLETENESS_ANALYSIS (analysis_id, [TABLE_CD], [DESCRIPTION], [NUM_RECORDS], [PERCENTAGE], date_of_testing)
SELECT 1
    ,'OBSERVATION_FACT'
	  ,'Group by Year: ' + [range]
	  ,[NUM_RECORDS]
	  ,(cast([NUM_RECORDS] as float)/cast(@DENOMINATOR_PAT as float))*100.0
	  , getDate()
from (
	select count_big(distinct patient_num) as [NUM_RECORDS]
		 , cast(year(start_date) as varchar) --, '-', right('0'+RTRIM(month(start_date)), 2)) 
as [range]
	from dbo.OBSERVATION_FACT
	WHERE (CONCEPT_CD NOT LIKE 'MRN%' OR CONCEPT_CD NOT LIKE 'SDH%' OR CONCEPT_CD NOT LIKE 'DEM|VITALS%')
	group by (year(start_date)) --, '-', right('0'+RTRIM(month(start_date)), 2))
) [analysis 1]

insert into ETL_PROV_DENSITY_ANALYSIS (analysis_id, [PROVIDER_ID], [NUM_RECORDS], [PERCENTAGE], date_of_testing)
SELECT 1	  
	  ,PROVIDER_ID
	  ,[NUM_RECORDS]
	  ,(cast([NUM_RECORDS] as float)/cast(@DENOMINATOR_PAT as float))*100
	  , getDate()
from (
	select COUNT_BIG(distinct patient_num) [NUM_RECORDS], PROVIDER_ID from
	dbo.OBSERVATION_FACT
	WHERE (CONCEPT_CD NOT LIKE 'MRN%' OR CONCEPT_CD NOT LIKE 'SDH%' OR CONCEPT_CD NOT LIKE 'DEM|VITALS%')
	group by provider_id
) [analysis 2]

insert into ETL_COMPLETENESS_ANALYSIS (analysis_id, [TABLE_CD], [DESCRIPTION], [NUM_RECORDS], [PERCENTAGE], date_of_testing)
SELECT 1
      ,'PATIENT_DIMENSION'
	  ,'Patients with at least one measurement, diagnosis, or medication recorded'
	  ,[NUM_RECORDS]
	  ,(cast([NUM_RECORDS] as float)/cast(@DENOMINATOR_PAT as float))*100
	  , getDate()
from (
	select COUNT_BIG(distinct patient_num) [NUM_RECORDS] 
	from DBO.PATIENT_DIMENSION pd
	WHERE exists (
		select patient_num from dbo.OBSERVATION_FACT [of]
		where (concept_cd LIKE 'ICD9:%' OR CONCEPT_CD LIKE 'ICD10:%')
		and [of].[patient_num] = [pd].[PATIENT_NUM]
	) 
	AND exists (
		select patient_num from dbo.OBSERVATION_FACT [of]
		where concept_cd like 'LOINC%'
		and [of].[patient_num] = [pd].[PATIENT_NUM]
	)
	AND exists (
		select patient_num from dbo.OBSERVATION_FACT [of]
		where (concept_cd LIKE 'RXNORM:%'
			OR CONCEPT_CD LIKE 'MULTUM%')
		and [of].[patient_num] = [pd].[PATIENT_NUM]
	)	
) [analysis 3]


insert into ETL_COMPLETENESS_ANALYSIS (analysis_id, [TABLE_CD], [DESCRIPTION], [NUM_RECORDS], [PERCENTAGE], date_of_testing)
SELECT  2
      ,'PATIENT_DIMENSION'
	  , 'Patients with no encounter'
	  , [patients_with_no_enc]
	  , (cast([patients_with_no_enc] as float)/cast(@DENOMINATOR_PAT as float))*100
	  , getDate()
from (
	select COUNT_BIG(distinct pd.patient_num) [patients_with_no_enc]
	from dbo.PATIENT_DIMENSION pd
	where patient_num not in ( select distinct patient_num from dbo.OBSERVATION_FACT )
) [analysis 4]


insert into ETL_COMPLETENESS_ANALYSIS (analysis_id, [TABLE_CD], [DESCRIPTION], [NUM_RECORDS], [PERCENTAGE], date_of_testing)
select  3
      ,'PATIENT_DIMENSION'
    , 'Race breakdown: ' + race_cd
    , race_breakdown
    , (cast([race_breakdown] as float)/cast(@DENOMINATOR_PAT as float))*100
	, getDate()
from (
	select COUNT_BIG(distinct patient_num) [race_breakdown], race_cd from
	dbo.PATIENT_DIMENSION
	group by race_cd
) [analysis 5]


insert into ETL_COMPLETENESS_ANALYSIS (analysis_id, [TABLE_CD], [DESCRIPTION], [NUM_RECORDS], [PERCENTAGE], date_of_testing)
select  4
      ,'PATIENT_DIMENSION'
    , 'Sex breakdown: ' + sex_cd
    , sex_breakdown
    , (cast([sex_breakdown] as float)/cast(@DENOMINATOR_PAT as float))*100
	, getDate()
from (
	select COUNT_BIG(distinct patient_num) [sex_breakdown], sex_cd from
	dbo.PATIENT_DIMENSION
	group by sex_cd
) [analysis 6]

insert into ETL_COMPLETENESS_ANALYSIS (analysis_id, [TABLE_CD], [DESCRIPTION], [NUM_RECORDS], [PERCENTAGE], date_of_testing)
select  5
      ,'PATIENT_DIMENSION'
    , 'Vital Status breakdown: ' + vital_status_cd
    , vital_status_cd_breakdown
    , (cast([vital_status_cd_breakdown] as float)/cast(@DENOMINATOR_PAT as float))*100
	, getDate()
from (
	select COUNT_BIG(distinct patient_num) [vital_status_cd_breakdown], vital_status_cd from
	dbo.PATIENT_DIMENSION
	group by vital_status_cd
) [analysis 7]


insert into ETL_COMPLETENESS_ANALYSIS (analysis_id, [TABLE_CD], [DESCRIPTION], [NUM_RECORDS], [PERCENTAGE], date_of_testing)
select  6
      ,'PATIENT_DIMENSION'
    , 'Age breakdown: ' + age
    , age_breakdown
    , (cast([age_breakdown] as float)/cast(@DENOMINATOR_PAT as float))*100
	, getDate()
from (

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
else 'unkn' end as age,
  //count(*) as count
 COUNT_BIG(*) [age_breakdown]
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
) [analysis 8 - Age]


DECLARE @DENOMINATOR_ENC bigint = (SELECT COUNT(distinct encounter_num) FROM dbo.visit_dimension)

insert into ETL_COMPLETENESS_ANALYSIS (analysis_id, [TABLE_CD], [DESCRIPTION], [NUM_RECORDS], [PERCENTAGE], date_of_testing)
SELECT  1
      ,'VISIT_DIMENSION'
	  , 'Encounter Type: ' + inout_cd
	  , [inout_brwakdown]
	  , (cast([inout_brwakdown] as float)/cast(@DENOMINATOR_ENC as float))*100
	  , getDate()
from (
	select COUNT_BIG(distinct pd.encounter_num) [inout_brwakdown], inout_cd
	from dbo.visit_dimension pd
	group by inout_cd
) [analysis 9 - In Out Code]

insert into ETL_COMPLETENESS_ANALYSIS (analysis_id, [TABLE_CD], [DESCRIPTION], [NUM_RECORDS], [PERCENTAGE], date_of_testing)
SELECT  2
      ,'VISIT_DIMENSION'
	  , 'Length of Stay: ' + length_of_stay
	  , [length_of_stay_brwakdown]
	  , (cast([length_of_stay_brwakdown] as float)/cast(@DENOMINATOR_ENC as float))*100
	  , getDate()
from (
	select COUNT_BIG(distinct pd.encounter_num) [length_of_stay_brwakdown], length_of_stay
	from dbo.visit_dimension pd
where length_of_stay != null
	group by length_of_stay
) [analysis 10 - Length of Stay]

insert into ETL_COMPLETENESS_ANALYSIS (analysis_id, [TABLE_CD], [DESCRIPTION], [NUM_RECORDS], [PERCENTAGE], date_of_testing)
SELECT  3
      ,'VISIT_DIMENSION'
	  , 'Encounter by Year: ' + years
	  , [years_breakdown]
	  , (cast([years_breakdown] as float)/cast(@DENOMINATOR_ENC as float))*100
	  , getDate()
from (
	select COUNT_BIG(distinct pd.encounter_num) [years_breakdown], convert(varchar(10), YEAR(START_DATE))  [years]
	from dbo.visit_dimension  pd
where start_date is not null
	group by YEAR(START_DATE)

) [analysis 11 - Year Breakdown]

insert into ETL_COMPLETENESS_ANALYSIS (analysis_id, [TABLE_CD], [DESCRIPTION], [NUM_RECORDS], [PERCENTAGE], date_of_testing)
SELECT  4
      ,'VISIT_DIMENSION'
	  , 'Encounter by Discharge Year: ' + years
	  , [years_breakdown]
	  , (cast([years_breakdown] as float)/cast(@DENOMINATOR_ENC as float))*100
	  , getDate()
from (
	select COUNT_BIG(distinct pd.encounter_num) [years_breakdown],  convert(varchar(10), YEAR(end_date)) [years]
	from dbo.visit_dimension pd
where start_date != end_date
and start_date is not null
	group by YEAR(end_date)
) [analysis 12 - Discharge Year Breakdown]

DECLARE @DENOMINATOR_STAT bigint = (SELECT COUNT(distinct query_master_id) FROM dbo.qt_query_master)

insert into ETL_COMPLETENESS_ANALYSIS (analysis_id, [TABLE_CD], [DESCRIPTION], [NUM_RECORDS], [PERCENTAGE], date_of_testing)
SELECT  1
      ,'STATS'
	  , 'Queries by Year: ' + years
	  , [years_breakdown]
	  , (cast([years_breakdown] as float)/cast(@DENOMINATOR_STAT as float))*100
	  , getDate()
from (
	select COUNT_BIG(distinct pd.query_master_id) [years_breakdown],  convert(varchar(10), YEAR(create_date)) [years]
	from dbo.qt_query_master pd
	group by YEAR(create_date)
) [analysis 13 - Queries Year Breakdown]

insert into ETL_COMPLETENESS_ANALYSIS (analysis_id, [TABLE_CD], [DESCRIPTION], [NUM_RECORDS], [PERCENTAGE], date_of_testing)
SELECT  2
      ,'STATS'
	  , 'Queries by Year and User: ' + years
	  , [years_breakdown]
	  , (cast([years_breakdown] as float)/cast(@DENOMINATOR_STAT as float))*100
	  , getDate()
from (
	select COUNT_BIG(distinct pd.query_master_id) [years_breakdown],  concat(convert(varchar(10), YEAR(create_date)), ' - ', user_id) [years]
	from dbo.qt_query_master pd
	group by YEAR(create_date), user_id
) [analysis 13 - Queries Year Breakdown]

insert into ETL_COMPLETENESS_ANALYSIS (analysis_id, [TABLE_CD], [DESCRIPTION], [NUM_RECORDS], [PERCENTAGE], date_of_testing)
SELECT  4
      ,'STATS'
	  , 'Batch Mode by Year and Type: ' + years
	  , [years_breakdown]
	  , (cast([years_breakdown] as float)/cast(@DENOMINATOR_STAT as float))*100
	  , getDate()
from (
	select COUNT_BIG(distinct pd.query_master_id) [years_breakdown],  concat(convert(varchar(10), YEAR(start_date)), ' - ', batch_mode) [years]
	from dbo.qt_query_instance pd
	group by YEAR(start_date), batch_mode
) [analysis 13 - Queries Batch Mode Year Breakdown]

DECLARE @DENOMINATOR_STAT_FINISHED bigint = (SELECT COUNT(distinct query_master_id) from qt_query_instance where batch_mode = 'FINISHED')

insert into ETL_COMPLETENESS_ANALYSIS (analysis_id, [TABLE_CD], [DESCRIPTION], [NUM_RECORDS], [PERCENTAGE], date_of_testing)
SELECT  5
      ,'STATS'
	  , 'Query Time by Year and Seconds: ' + age
	  , [years_breakdown]
	  , (cast([years_breakdown] as float)/cast(@DENOMINATOR_STAT_FINISHED as float))*100
	  , getDate()
from (



select
   case when age < 0 then concat(years, ' - ', 'Under 0 second' )
       when age = 0 then concat(years, ' - ', '0 second' )
       when age < 1 and age > 0 then concat(years, ' - ', '0-1 seconds')
       when age < 5 and age > 1 then concat(years, ' - ', '2-4 seconds')
       when age < 10 and age > 4 then concat(years, ' - ', '5-9 seconds')
       when age < 15 and age > 9 then concat(years, ' - ', '10-14 seconds')
       when age < 19 and age > 14 then concat(years, ' - ', '15-18 seconds')
       when age < 22 and age > 18 then concat(years, ' - ', '19-21 seconds')
       when age < 45 and age > 21 then concat(years, ' - ', '22-44 seconds')
       when age < 65 and age > 44 then concat(years, ' - ', '45-64 seconds')
       when age < 75 and age > 64 then concat(years, ' - ', '65-74 seconds')
       when age < 90 and age > 74 then concat(years, ' - ', '75-89 seconds')
       when age > 89 then  concat(years, ' - ', 'Over 89 seconds' )
else concat(years, ' - ', 'unkn') end as age,
  //count(*) as count
 COUNT_BIG(*) [years_breakdown]
from (select DATEDIFF(ss, start_date, end_date) as age, YEAR(start_date) as years from qt_query_instance where batch_mode = 'FINISHED') c
group by   case when age < 0 then concat(years, ' - ', 'Under 0 second' )
       when age = 0 then concat(years, ' - ', '0 second' )
       when age < 1 and age > 0 then concat(years, ' - ', '0-1 seconds')
       when age < 5 and age > 1 then concat(years, ' - ', '2-4 seconds')
       when age < 10 and age > 4 then concat(years, ' - ', '5-9 seconds')
       when age < 15 and age > 9 then concat(years, ' - ', '10-14 seconds')
       when age < 19 and age > 14 then concat(years, ' - ', '15-18 seconds')
       when age < 22 and age > 18 then concat(years, ' - ', '19-21 seconds')
       when age < 45 and age > 21 then concat(years, ' - ', '22-44 seconds')
       when age < 65 and age > 44 then concat(years, ' - ', '45-64 seconds')
       when age < 75 and age > 64 then concat(years, ' - ', '65-74 seconds')
       when age < 90 and age > 74 then concat(years, ' - ', '75-89 seconds')
       when age > 89 then  concat(years, ' - ', 'Over 89 seconds' )
   else  concat(years, ' - ', 'unkn') end

) [analysis 14 - Queries year runtime breakdown]


