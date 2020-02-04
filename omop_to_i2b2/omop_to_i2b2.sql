

create   PROCEDURE [dbo].[omop_src_i2b2_rebuild_metadata_index]
AS
  BEGIN

  
DECLARE @sqlstr NVARCHAR(4000);
DECLARE @sqltext NVARCHAR(4000);
DECLARE @sqlcurs NVARCHAR(4000);

--IF COL_LENGTH('table_access','c_obsfact') is NOT NULL 
--declare getsql cursor local for
--select 'exec run_all_counts '+c_table_name+','+c_obsfact from TABLE_ACCESS where c_visualattributes like '%A%' 
--ELSE 
declare getsql cursor local for select distinct c_table_name from TABLE_ACCESS where c_visualattributes like '%A%'


begin
OPEN getsql;
FETCH NEXT FROM getsql INTO @sqltext;
WHILE @@FETCH_STATUS = 0
BEGIN

	      BEGIN TRY   
         SET @sqlstr = 'ALTER INDEX ALL ON ' + @sqltext + ' REBUILD' 
         --PRINT @cmd -- uncomment if you want to see commands
         EXEC (@sqlstr) 
      END TRY
      BEGIN CATCH
         PRINT '---'
         PRINT @sqlstr
         PRINT ERROR_MESSAGE() 
         PRINT '---'
      END CATCH


	FETCH NEXT FROM getsql INTO @sqltext;
	END
   CLOSE getsql   
   DEALLOCATE getsql  


   end

END
GO


create   PROCEDURE [dbo].[omop_src_i2b2_transmaart_create_index]
AS
  BEGIN

  
CREATE  INDEX EM_IDX_ENCPATH ON ENCOUNTER_MAPPING(ENCOUNTER_IDE, ENCOUNTER_IDE_SOURCE, PATIENT_IDE, PATIENT_IDE_SOURCE, ENCOUNTER_NUM)
;
CREATE  INDEX EM_IDX_UPLOADID ON ENCOUNTER_MAPPING(UPLOAD_ID)
;
CREATE INDEX EM_ENCNUM_IDX ON ENCOUNTER_MAPPING(ENCOUNTER_NUM)
;

CREATE  INDEX PM_IDX_UPLOADID ON PATIENT_MAPPING(UPLOAD_ID)
;
CREATE INDEX PM_PATNUM_IDX ON PATIENT_MAPPING(PATIENT_NUM)
;
CREATE INDEX PM_ENCPNUM_IDX ON 
PATIENT_MAPPING(PATIENT_IDE,PATIENT_IDE_SOURCE,PATIENT_NUM) ;


/* add index on concept_cd */
CREATE CLUSTERED INDEX OF_IDX_ClusteredConcept ON OBSERVATION_FACT
(
	CONCEPT_CD 
)
;

/* add an index on most of the observation_fact fields */
CREATE INDEX OF_IDX_ALLObservation_Fact ON OBSERVATION_FACT
(
	PATIENT_NUM ,
	ENCOUNTER_NUM ,
	CONCEPT_CD ,
	START_DATE ,
	PROVIDER_ID ,
	MODIFIER_CD ,
	INSTANCE_NUM,
	VALTYPE_CD ,
	TVAL_CHAR ,
	NVAL_NUM ,
	VALUEFLAG_CD ,
	QUANTITY_NUM ,
	UNITS_CD ,
	END_DATE ,
	LOCATION_CD ,
	CONFIDENCE_NUM
)
;
/* add additional indexes on observation_fact fields */
CREATE INDEX OF_IDX_Start_Date ON OBSERVATION_FACT(START_DATE, PATIENT_NUM)
;
CREATE INDEX OF_IDX_Modifier ON OBSERVATION_FACT(MODIFIER_CD)
;
CREATE INDEX OF_IDX_Encounter_Patient ON OBSERVATION_FACT(ENCOUNTER_NUM, PATIENT_NUM, INSTANCE_NUM)
;
CREATE INDEX OF_IDX_UPLOADID ON OBSERVATION_FACT(UPLOAD_ID)
;
CREATE INDEX OF_IDX_SOURCESYSTEM_CD ON OBSERVATION_FACT(SOURCESYSTEM_CD)
;
/* add indexes on additional PATIENT_DIMENSION fields */
CREATE  INDEX PD_IDX_DATES ON PATIENT_DIMENSION(PATIENT_NUM, VITAL_STATUS_CD, BIRTH_DATE, DEATH_DATE)
;
CREATE  INDEX PD_IDX_AllPatientDim ON PATIENT_DIMENSION(PATIENT_NUM, VITAL_STATUS_CD, BIRTH_DATE, DEATH_DATE, SEX_CD, AGE_IN_YEARS_NUM, LANGUAGE_CD, RACE_CD, MARITAL_STATUS_CD, INCOME_CD, RELIGION_CD, ZIP_CD)
;
CREATE  INDEX PD_IDX_StateCityZip ON PATIENT_DIMENSION (STATECITYZIP_PATH, PATIENT_NUM)
;
CREATE INDEX PA_IDX_UPLOADID ON PATIENT_DIMENSION(UPLOAD_ID)
;

/* add index on PROVIDER_ID, NAME_CHAR */
CREATE INDEX PD_IDX_NAME_CHAR ON PROVIDER_DIMENSION(PROVIDER_ID, NAME_CHAR)
;
CREATE INDEX PD_IDX_UPLOADID ON PROVIDER_DIMENSION(UPLOAD_ID)
;

/* add indexes on addtional visit_dimension fields */
CREATE  INDEX VD_IDX_DATES ON VISIT_DIMENSION(ENCOUNTER_NUM, START_DATE, END_DATE)
;
CREATE  INDEX VD_IDX_AllVisitDim ON VISIT_DIMENSION(ENCOUNTER_NUM, PATIENT_NUM, INOUT_CD, LOCATION_CD, START_DATE, LENGTH_OF_STAY, END_DATE)
;
CREATE  INDEX VD_IDX_UPLOADID ON VISIT_DIMENSION(UPLOAD_ID)
;


  END

GO

create   PROCEDURE [dbo].[omop_src_i2b2_transmaart_drop_index]
AS
  BEGIN

  
DROP  INDEX EM_IDX_ENCPATH ON ENCOUNTER_MAPPING
;
DROP  INDEX EM_IDX_UPLOADID ON ENCOUNTER_MAPPING
;
DROP INDEX EM_ENCNUM_IDX ON ENCOUNTER_MAPPING
;

DROP  INDEX PM_IDX_UPLOADID ON PATIENT_MAPPING
;
DROP INDEX PM_PATNUM_IDX ON PATIENT_MAPPING
;
DROP INDEX PM_ENCPNUM_IDX ON 
PATIENT_MAPPING ;


/* add index on concept_cd */
DROP INDEX OF_IDX_ClusteredConcept ON OBSERVATION_FACT

;

/* add an index on most of the observation_fact fields */
DROP INDEX OF_IDX_ALLObservation_Fact ON OBSERVATION_FACT

;
/* add additional indexes on observation_fact fields */
DROP INDEX OF_IDX_Start_Date ON OBSERVATION_FACT
;
DROP INDEX OF_IDX_Modifier ON OBSERVATION_FACT
;
DROP INDEX OF_IDX_Encounter_Patient ON OBSERVATION_FACT
;
DROP INDEX OF_IDX_UPLOADID ON OBSERVATION_FACT
;
DROP INDEX OF_IDX_SOURCESYSTEM_CD ON OBSERVATION_FACT
;

/* add indexes on additional PATIENT_DIMENSION fields */
DROP  INDEX PD_IDX_DATES ON PATIENT_DIMENSION
;
DROP  INDEX PD_IDX_AllPatientDim ON PATIENT_DIMENSION
;
DROP  INDEX PD_IDX_StateCityZip ON PATIENT_DIMENSION
;
DROP INDEX PA_IDX_UPLOADID ON PATIENT_DIMENSION
;

/* add index on PROVIDER_ID, NAME_CHAR */
DROP INDEX PD_IDX_NAME_CHAR ON PROVIDER_DIMENSION
;
DROP INDEX PD_IDX_UPLOADID ON PROVIDER_DIMENSION
;

/* add indexes on addtional visit_dimension fields */
DROP  INDEX VD_IDX_DATES ON VISIT_DIMENSION
;
DROP  INDEX VD_IDX_AllVisitDim ON VISIT_DIMENSION
;
DROP  INDEX VD_IDX_UPLOADID ON VISIT_DIMENSION
;



  END

GO

CREATE   PROCEDURE [dbo].[omop_src_i2b2_transmaart_encryption]
AS
  BEGIN

  create master key encryption by password = 'My1Strong2Passowrd@';

  CREATE CERTIFICATE PHI  
   WITH SUBJECT = 'Personal Health Information';  
;


 CREATE SYMMETRIC KEY PHI_Key11  
    WITH ALGORITHM = AES_128 
    ENCRYPTION BY CERTIFICATE PHI;  
;  



   END

GO

create   PROCEDURE [dbo].[omop_src_i2b2_transmaart_patient_dimension]
AS
  BEGIN

insert into PATIENT_DIMENSION
(PATIENT_NUM,
BIRTH_DATE,
DEATH_DATE,
VITAL_STATUS_CD,
SEX_CD,
race_cd
)
select distinct person_id, birth_datetime, death_datetime,
 case   
 when (death_datetime is not null) then 'Y'
 else 'N'
 end,
gender_source_value,
race_source_value
from wz_src..person


INSERT INTO OBSERVATION_FACT
           (ENCOUNTER_NUM
           ,PATIENT_NUM
           ,CONCEPT_CD
           ,PROVIDER_ID
           ,START_DATE
           ,MODIFIER_CD
           ,INSTANCE_NUM
           ,VALTYPE_CD
           ,TVAL_CHAR
           ,NVAL_NUM
           ,VALUEFLAG_CD
           ,QUANTITY_NUM
           ,UNITS_CD
           ,END_DATE
           ,LOCATION_CD
           ,OBSERVATION_BLOB
           ,CONFIDENCE_NUM
           ,UPDATE_DATE
           ,DOWNLOAD_DATE
           ,IMPORT_DATE
           ,SOURCESYSTEM_CD
           ,UPLOAD_ID)
     select distinct
	 -1,
	 o.person_id,
	 case when o.ethnicity_source_value = 'Not Hispanic or Latino' then 'DEM|HISP:N'
	 when o.ethnicity_source_value = 'Hispanic or Latino' then 'DEM|HISP:Y'
	  else 'DEM|HISP:NI' END,
	 '@',
	o.birth_datetime,
	 '@',
	 1,
	 null,
	null,
	 null,
	null,
	1,
	null,
	null,
	null,
	null,
	1,
	getdate(),
	getdate(),
	getdate(),
	'OMOP',
	0

	from wz_src..person o

END

GO

CREATE   PROCEDURE [dbo].[omop_src_i2b2_transmaart_patient_mapping]
AS
  BEGIN

   

INSERT INTO dbo.PATIENT_MAPPING
           (PATIENT_IDE
           ,PATIENT_IDE_SOURCE
           ,PATIENT_NUM
           ,PATIENT_IDE_STATUS
           ,PROJECT_ID
           ,UPLOAD_DATE
           ,UPDATE_DATE
           ,DOWNLOAD_DATE
           ,IMPORT_DATE
           ,SOURCESYSTEM_CD
           ,UPLOAD_ID)
     select distinct 
           p.person_id,
           'HIVE',
           p.person_id,
           'A',
           '@',
           getdate(),
           getdate(),
           getdate(),
           getdate(),
		   'OMOP',
           '1'
		   from wz_src..person p;

		   
OPEN SYMMETRIC KEY PHI_Key11  
   DECRYPTION BY CERTIFICATE PHI;  

INSERT INTO dbo.PATIENT_MAPPING
           (PATIENT_IDE
           ,PATIENT_IDE_SOURCE
           ,PATIENT_NUM
           ,PATIENT_IDE_STATUS
           ,PROJECT_ID
           ,UPLOAD_DATE
           ,UPDATE_DATE
           ,DOWNLOAD_DATE
           ,IMPORT_DATE
           ,SOURCESYSTEM_CD
           ,UPLOAD_ID)

		   
     select
	 EncryptByKey(key_GUID('PHI_Key11' ) ,convert(nvarchar(200)  , p.person_source_value)),
           'NEXTGEN',
           p.person_id,
           'A',
           '@',
           getdate(),
           getdate(),
           getdate(),
           getdate(),
		   'OMOP',
           '1'

		
		   from wz_src..person p

		   END



GO

  create   PROCEDURE [dbo].[omop_src_i2b2_transmaart_visit_dimension]
AS
  BEGIN


INSERT INTO VISIT_DIMENSION
           (ENCOUNTER_NUM
           ,PATIENT_NUM
           ,ACTIVE_STATUS_CD
           ,START_DATE
           ,END_DATE
           ,INOUT_CD
           ,LOCATION_CD
           ,LOCATION_PATH
           ,LENGTH_OF_STAY
           ,VISIT_BLOB
           ,UPDATE_DATE
           ,DOWNLOAD_DATE
           ,IMPORT_DATE
           ,SOURCESYSTEM_CD
           ,UPLOAD_ID)
		   select distinct
		   visit_detail_parent_id,
		   person_id,
		   'A',
		   visit_detail_start_datetime,
		   visit_detail_end_datetime,
		   visit_detail_source_value,
		   care_site_id,
		    null,
			0,
			null,

		   	getdate(),
	getdate(),
	getdate(),
	'OMOP',
	0
		   from  wz_src..visit_detail


  END

GO

  CREATE    PROCEDURE [dbo].[omop_src_i2b2_transmart_diagnosis]
AS
  BEGIN

INSERT INTO OBSERVATION_FACT
           (ENCOUNTER_NUM
           ,PATIENT_NUM
           ,CONCEPT_CD
           ,PROVIDER_ID
           ,START_DATE
           ,MODIFIER_CD
           ,INSTANCE_NUM
           ,VALTYPE_CD
           ,TVAL_CHAR
           ,NVAL_NUM
           ,VALUEFLAG_CD
           ,QUANTITY_NUM
           ,UNITS_CD
           ,END_DATE
           ,LOCATION_CD
           ,OBSERVATION_BLOB
           ,CONFIDENCE_NUM
           ,UPDATE_DATE
           ,DOWNLOAD_DATE
           ,IMPORT_DATE
           ,SOURCESYSTEM_CD
           ,UPLOAD_ID)
     select distinct
	 o.observation_id,
	 o.person_id,
	  o.observation_source_value,
	 o.provider_id,
	 o.observation_datetime,
	 '@',
	 1,
	 case when (o.value_as_number is not null) then 'N'
	 when ( o.value_as_string is not null) then 'T'
	 else null
	 END,
	 o.value_as_string,
	 o.value_as_number,
	null,
	1,
	null,
	null,
	null,
	null,
	1,
	getdate(),
	getdate(),
	getdate(),
	'OMOP',
	0

	from wz_src..observation o
end

GO

creaTe     PROCEDURE [dbo].[omop_src_i2b2_transmart_lab]
AS
  BEGIN

INSERT INTO OBSERVATION_FACT
           (ENCOUNTER_NUM
           ,PATIENT_NUM
           ,CONCEPT_CD
           ,PROVIDER_ID
           ,START_DATE
           ,MODIFIER_CD
           ,INSTANCE_NUM
           ,VALTYPE_CD
           ,TVAL_CHAR
           ,NVAL_NUM
           ,VALUEFLAG_CD
           ,QUANTITY_NUM
           ,UNITS_CD
           ,END_DATE
           ,LOCATION_CD
           ,OBSERVATION_BLOB
           ,CONFIDENCE_NUM
           ,UPDATE_DATE
           ,DOWNLOAD_DATE
           ,IMPORT_DATE
           ,SOURCESYSTEM_CD
           ,UPLOAD_ID)
      select distinct
	 o.visit_occurrence_id,
	 o.person_id,
	 concat('LOINC:', o.value_source_value),
	 o.provider_id,
	 o.measurement_datetime,
	 '@',
	 1,
	 case when (o.value_as_number is not null) then 'N'
	 else 'T'
	 END,
	 case when (o.value_as_number is  null) then o.measurement_source_value
	 else NULL
	 END,
	 case when (o.value_as_number is not null) then o.value_as_number
	 else NULL
	 END,
	null,
	1,
	null,
	null,
	null,
	null,
	1,
	getdate(),
	getdate(),
	getdate(),
	'OMOP',
	0

	from wz_src..measurement o
end

GO

create    PROCEDURE [dbo].[omop_src_i2b2_transmart_medication]
AS
  BEGIN

INSERT INTO OBSERVATION_FACT
           (ENCOUNTER_NUM
           ,PATIENT_NUM
           ,CONCEPT_CD
           ,PROVIDER_ID
           ,START_DATE
           ,MODIFIER_CD
           ,INSTANCE_NUM
           ,VALTYPE_CD
           ,TVAL_CHAR
           ,NVAL_NUM
           ,VALUEFLAG_CD
           ,QUANTITY_NUM
           ,UNITS_CD
           ,END_DATE
           ,LOCATION_CD
           ,OBSERVATION_BLOB
           ,CONFIDENCE_NUM
           ,UPDATE_DATE
           ,DOWNLOAD_DATE
           ,IMPORT_DATE
           ,SOURCESYSTEM_CD
           ,UPLOAD_ID)
     select distinct
	 o.visit_occurrence_id,
	 o.person_id,
	  o.drug_source_value,
	 o.provider_id,
	 o.drug_exposure_start_datetime,
	 '@',
	 1,
	 null,
	 o.drug_exposure_end_datetime,
	 null,
	null,
	o.refills,
	null,
	null,
	null,
	null,
	1,
	getdate(),
	getdate(),
	getdate(),
	'OMOP',
	0

	from wz_src..drug_exposure o
end

GO

 CREATE   PROCEDURE [dbo].[omop_src_i2b2_transmart_provider]
AS
  BEGIN
      INSERT INTO provider_dimension
                  (provider_id,
                   provider_path,
                   name_char
                   ,UPDATE_DATE
           ,DOWNLOAD_DATE
           ,IMPORT_DATE
           ,SOURCESYSTEM_CD
           ,UPLOAD_ID)
      SELECT a.provider_id AS provider_id,
             a.provider_name,
            a.provider_name,
            
	getdate(),
	getdate(),
	getdate(),
	'OMOP',
	0      
      FROM  wz_src..provider a
  END  
 

GO
