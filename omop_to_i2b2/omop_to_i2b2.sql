
create or alter PROCEDURE [dbo].[omop_src_i2b2_transmaart_patient_dimension]
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

go

CREATE OR ALTER PROCEDURE [dbo].[omop_src_i2b2_transmaart_encryption]
AS
  BEGIN

  create master key encryption by password = 'My1Strong2Passowrd@';

  CREATE CERTIFICATE PHI  
   WITH SUBJECT = 'Personal Health Information';  
;

DROP  SYMMETRIC KEY PHI_Key11  

 CREATE SYMMETRIC KEY PHI_Key11  
    WITH ALGORITHM = AES_128 
    ENCRYPTION BY CERTIFICATE PHI;  
;  


OPEN SYMMETRIC KEY PHI_Key11  
   DECRYPTION BY CERTIFICATE PHI;  

   END

   go

CREATE OR ALTER PROCEDURE [dbo].[omop_src_i2b2_transmaart_patient_mapping]
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
           'NEXTGEN_E',
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

  CREATE OR ALTER  PROCEDURE [dbo].[omop_src_i2b2_transmart_diagnosis]
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

go

creaTe  OR ALTER  PROCEDURE [dbo].[omop_src_i2b2_transmart_lab]
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

go

create  OR ALTER PROCEDURE [dbo].[omop_src_i2b2_transmart_medication]
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

go

 CREATE OR ALTER PROCEDURE [dbo].[omop_src_i2b2_transmart_provider]
AS
  BEGIN
      INSERT INTO provider
                  (provider_id,
                   provider_name,
                   npi,
                   dea,
                   specialty_concept_id,
                   care_site_id,
                   year_of_birth,
                   gender_concept_id,
                   provider_source_value,
                   specialty_source_value,
                   specialty_source_concept_id,
                   gender_source_value,
                   gender_source_concept_id)
      SELECT a.provider_id AS provider_id,
             Concat(a.first_name, ' ', a.last_name),
             a.national_provider_id,
             NULL,
			 NULL,
             0,
             NULL,
             NULL,
             0,
             NULL,
             NULL,
             0,
             NULL          
      FROM   lz_src..provider_mstr a
  END  
 

GO
