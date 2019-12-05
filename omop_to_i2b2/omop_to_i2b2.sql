
create PROCEDURE [dbo].[omop_src_i2b2_patient_dimension]
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
from person

END
