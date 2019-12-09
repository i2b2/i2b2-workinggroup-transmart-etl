
create PROCEDURE [dbo].[omop_src_i2b2_transmaart_patient_dimension]
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




  CREATE   PROCEDURE [dbo].[omop_src_i2b2_transmart_observation]
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

create       PROCEDURE [dbo].[omop_src_i2b2_transmart_medication]
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

