
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
