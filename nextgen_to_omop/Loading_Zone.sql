 CREATE PROCEDURE [dbo].[Lz_src_nextgen_omop_provider]
AS
  BEGIN
      INSERT INTO provider
                  (
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
      SELECT 
             Concat(a.first_name, ' ', a.last_name),
             a.national_provider_id,
             NULL,
			 NULL,
             0,
             NULL,
             NULL,
             a.provider_id,
             NULL,
             NULL,
            NULL,
             0          
      FROM   lz_src..provider_mstr a
  END  
 
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create   PROCEDURE [dbo].[wz_src_nextgen_omop_care_site]
AS
  BEGIN
INSERT INTO location
           (address_1
           ,address_2
           ,city
           ,state
           ,zip
           ,county
           ,country
           ,location_source_value
           ,latitude
           ,longitude)
     
	 select 
	   address_line_1,
	   address_line_2,
	   city,
	   state,
	   zip,
	   null,
	   null,
	   location_id,
	   null,
	   null
	  FROM   lz_src..lz_nextgen_location_mstr a

INSERT INTO care_site
           (
           care_site_name
           ,place_of_service_concept_id
           ,location_id
           ,care_site_source_value
           ,place_of_service_source_value)
     select 
	 a.location_name,
	 0,
	 l.location_id,
	 a.location_id,
	 null
	  FROM   lz_src..lz_nextgen_location_mstr a
	  JOIN       location l
ON         a.location_id = l.location_source_value


END

GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


   CREATE   PROCEDURE [dbo].[wz_src_nextgen_omop_diagnosis]
AS
  BEGIN
    IF EXISTS
    (
           SELECT 1
           FROM   sysobjects
           WHERE  xtype='u'
           AND    NAME='tmp_nextgen_diagnosis_encounter')
    DROP TABLE tmp_nextgen_diagnosis_encounter
    SELECT DISTINCT p.person_id,
				    s.person_nbr,
                    i.enc_nbr,
                    p.enc_id
    INTO            tmp_nextgen_diagnosis_encounter
    FROM            lz_src..lz_nextgen_patient_diagnosis p
    JOIN            lz_src..lz_nextgen_patient_encounter i
    ON              i.enc_id=p.enc_id
    JOIN            lz_src..lz_nextgen_person s
    ON              s.person_id=p.person_id	
	

INSERT INTO observation
            (
                        observation_id,
                        person_id,
                        observation_concept_id,
                        observation_date,
                        observation_datetime,
                        observation_type_concept_id,
                        value_as_number,
                        value_as_string,
                        value_as_concept_id,
                        qualifier_concept_id,
                        unit_concept_id,
                        provider_id,
                        visit_occurrence_id,
                        visit_detail_id,
                        observation_source_value,
                        observation_source_concept_id,
                        unit_source_value,
                        qualifier_source_value,
						observation_event_id,
						obs_event_field_concept_id,
						value_as_datetime
            )
SELECT    
0,
           p.person_nbr AS patient_id,
		   0,
		   NULL,
           CASE
                      WHEN Isdate(NULLIF(a.create_timestamp,'')) = 1 THEN a.create_timestamp
                      ELSE GETDATE()
           END AS diag_date,
		   
            38000280,
           NULL, 
           null,
          null,
		   null,
           null,
           0, --provider_id
		   0,
	0,
case when e9.icd_type = '09' then concat('ICD9CM:', e9.icd9cm_code_id)
	else  concat('ICD', e9.icd_type, 'CM:', e9.icd9cm_code_id)
	 end,
	e9.icd_type,
	0,
	null,
	null,
            0,
           null
FROM       lz_src..lz_nextgen_patient_diagnosis a
JOIN       tmp_nextgen_diagnosis_encounter e
ON         e.person_id=a.person_id
AND        e.enc_id=a.enc_id
JOIN       lz_src..lz_nextgen_person p
ON         p.person_id=a.person_id
INNER JOIN lz_src..lz_nextgen_icd9cm_code_mstr e9
ON         a.icd9cm_code_id=e9.icd9cm_code_id
where e9.icd9cm_code_id is not null


INSERT INTO visit_detail
            (
                        visit_detail_id ,
                        person_id ,
                        visit_detail_concept_id ,
                        visit_detail_start_date ,
                        visit_detail_start_datetime ,
                        visit_detail_end_date ,
                        visit_detail_end_datetime ,
                        visit_detail_type_concept_id ,
                        provider_id ,
                        care_site_id ,
                        discharge_to_concept_id ,
                        admitted_from_concept_id ,
                        admitted_from_source_value ,
                        visit_detail_source_value ,
                        visit_detail_source_concept_id ,
                        discharge_to_source_value ,
                        preceding_visit_detail_id ,
                        visit_detail_parent_id ,
                        visit_occurrence_id
            )
SELECT  
el.enc_nbr    AS encounter_id,
         Max(e.person_nbr) AS patient_id,
         0,
         NULL,
         Min(
         CASE
                  WHEN el.admit_date IS NOT NULL THEN el.admit_date
                  ELSE el.enc_timestamp
         END) AS encounter_start_date,
         NULL,
         Max(
         CASE
                  WHEN el.discharge_date IS NOT NULL THEN el.discharge_date
                  ELSE el.enc_timestamp
         END) AS encounter_end_date,
         0,
         1,
         l.care_site_id,
         0,
         3,
         4,
         NULL,
         0 ,
         NULL,
         NULL,
         NULL,
		 0
FROM     lz_src..lz_nextgen_patient_encounter el
JOIN       tmp_nextgen_diagnosis_encounter e
ON         e.person_id=el.person_id
and		   e.enc_id=el.enc_id
JOIN       care_site l
ON         l.care_site_source_value=el.location_id
GROUP BY el.enc_nbr, l.care_site_id

END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


 create      PROCEDURE [dbo].[wz_src_nextgen_omop_lab]
AS
  BEGIN
    IF EXISTS
    (
           SELECT 1
           FROM   sysobjects
           WHERE  xtype='u'
           AND    NAME='tmp_nextgen_lab_encounter')
    DROP TABLE tmp_nextgen_lab_encounter
    SELECT DISTINCT p.person_id,
				    s.person_nbr,
                    i.enc_nbr,
                    p.enc_id
    INTO            tmp_nextgen_lab_encounter
    FROM            lz_src..lz_nextgen_lab_nor p
    JOIN            lz_src..lz_nextgen_patient_encounter i
    ON              i.enc_id=p.enc_id
    JOIN            lz_src..lz_nextgen_person s
    ON              s.person_id=p.person_id	


INSERT INTO measurement
           (person_id
           ,measurement_concept_id
           ,measurement_date
           ,measurement_datetime
           ,measurement_time
           ,measurement_type_concept_id
           ,operator_concept_id
           ,value_as_number
           ,value_as_concept_id
           ,unit_concept_id
           ,range_low
           ,range_high
           ,provider_id
           ,visit_occurrence_id
           ,visit_detail_id
           ,measurement_source_value
           ,measurement_source_concept_id
           ,unit_source_value
           ,value_source_value)
    select p.person_nbr,
          0,
		  null,
          r.coll_date_time,
          	  null,
  null,--        r.coll_date_time,
         
          0,
          null ,--<value_as_number, float,>
          null,-- ,<value_as_concept_id, int,>
          null,-- ,<unit_concept_id, int,>
          null,-- ,<range_low, float,>
           null,--,<range_high, float,>
           0, --n.ordering_provider
           e.enc_nbr,
           0,
           null, --,<measurement_source_value, varchar(50),>
           null,--,<measurement_source_concept_id, int,>
           null,--,<unit_source_value, varchar(50),>
           a.obs_id

FROM       lz_src..lz_nextgen_lab_results_obx a
JOIN       tmp_nextgen_lab_encounter e
ON         e.person_id=a.person_id
JOIN       lz_src..lz_nextgen_person p
ON         p.person_id=a.person_id
left join lz_src..lz_nextgen_lab_results_obr_p r on r.unique_obr_num=a.unique_obr_num
left join lz_src..lz_nextgen_lab_nor n on n.order_num=r.ngn_order_num


INSERT INTO visit_detail
            (
                        visit_detail_id ,
                        person_id ,
                        visit_detail_concept_id ,
                        visit_detail_start_date ,
                        visit_detail_start_datetime ,
                        visit_detail_end_date ,
                        visit_detail_end_datetime ,
                        visit_detail_type_concept_id ,
                        provider_id ,
                        care_site_id ,
                        discharge_to_concept_id ,
                        admitted_from_concept_id ,
                        admitted_from_source_value ,
                        visit_detail_source_value ,
                        visit_detail_source_concept_id ,
                        discharge_to_source_value ,
                        preceding_visit_detail_id ,
                        visit_detail_parent_id ,
                        visit_occurrence_id
            )
SELECT  
el.enc_nbr    AS encounter_id,
         Max(e.person_nbr) AS patient_id,
         0,
         NULL,
         Min(
         CASE
                  WHEN el.admit_date IS NOT NULL THEN el.admit_date
                  ELSE el.enc_timestamp
         END) AS encounter_start_date,
         NULL,
         Max(
         CASE
                  WHEN el.discharge_date IS NOT NULL THEN el.discharge_date
                  ELSE el.enc_timestamp
         END) AS encounter_end_date,
         0,
         1,
         l.care_site_id,
         0,
         3,
         4,
         NULL,
         0 ,
         NULL,
         NULL,
         NULL,
		 0
FROM     lz_src..lz_nextgen_patient_encounter el
JOIN       tmp_nextgen_diagnosis_encounter e
ON         e.person_id=el.person_id
and		   e.enc_id=el.enc_id
JOIN       care_site l
ON         l.care_site_source_value=el.location_id
GROUP BY el.enc_nbr, l.care_site_id

END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


 create      PROCEDURE [dbo].[wz_src_nextgen_omop_medication]
AS
  BEGIN
    IF EXISTS
    (
           SELECT 1
           FROM   sysobjects
           WHERE  xtype='u'
           AND    NAME='tmp_nextgen_medication_encounter')
    DROP TABLE tmp_nextgen_medication_encounter
    SELECT DISTINCT p.person_id,
				    s.person_nbr,
                    i.enc_nbr,
                    p.enc_id
    INTO            tmp_nextgen_medication_encounter
    FROM            lz_src..lz_nextgen_medication p
    JOIN            lz_src..lz_nextgen_patient_encounter i
    ON              i.enc_id=p.enc_id
    JOIN            lz_src..lz_nextgen_person s
    ON              s.person_id=p.person_id	

INSERT INTO drug_exposure
           (person_id
           ,drug_concept_id
           ,drug_exposure_start_date
           ,drug_exposure_start_datetime
           ,drug_exposure_end_date
           ,drug_exposure_end_datetime
           ,verbatim_end_date
           ,drug_type_concept_id
           ,stop_reason
           ,refills
           ,quantity
           ,days_supply
           ,sig
           ,route_concept_id
           ,lot_number
           ,provider_id
           ,visit_occurrence_id
           ,visit_detail_id
           ,drug_source_value
           ,drug_source_concept_id
           ,route_source_value
           ,dose_unit_source_value)
select
			p.person_nbr,
			0,
			null,
			a.create_timestamp,
			null,
       CASE
                      WHEN Isdate(NULLIF(a.date_stopped,'')) = 1 THEN a.date_stopped
                      ELSE a.create_timestamp
           END AS end_date,
           null,
           0,
           null,
          null,-- a.rx_refills,
          null,-- a.rx_quanity,
           null,
           a.sig_desc,
           0,
          null,
           0, --,<provider_id, bigint,>
           e.enc_nbr,
          0,
          concat('NDC:', a.ndc_id),
          0,
         null,
		 null
FROM       lz_src..lz_nextgen_medication a
JOIN       tmp_nextgen_medication_encounter e
ON         e.person_id=a.person_id
AND        e.enc_id=a.enc_id
JOIN       lz_src..lz_nextgen_person p
ON         p.person_id=a.person_id

INSERT INTO visit_detail
            (
                        visit_detail_id ,
                        person_id ,
                        visit_detail_concept_id ,
                        visit_detail_start_date ,
                        visit_detail_start_datetime ,
                        visit_detail_end_date ,
                        visit_detail_end_datetime ,
                        visit_detail_type_concept_id ,
                        provider_id ,
                        care_site_id ,
                        discharge_to_concept_id ,
                        admitted_from_concept_id ,
                        admitted_from_source_value ,
                        visit_detail_source_value ,
                        visit_detail_source_concept_id ,
                        discharge_to_source_value ,
                        preceding_visit_detail_id ,
                        visit_detail_parent_id ,
                        visit_occurrence_id
            )
SELECT  
el.enc_nbr    AS encounter_id,
         Max(e.person_nbr) AS patient_id,
         0,
         NULL,
         Min(
         CASE
                  WHEN el.admit_date IS NOT NULL THEN el.admit_date
                  ELSE el.enc_timestamp
         END) AS encounter_start_date,
         NULL,
         Max(
         CASE
                  WHEN el.discharge_date IS NOT NULL THEN el.discharge_date
                  ELSE el.enc_timestamp
         END) AS encounter_end_date,
         0,
         1,
         l.care_site_id,
         0,
         3,
         4,
         NULL,
         0 ,
         NULL,
         NULL,
         NULL,
		 0
FROM     lz_src..lz_nextgen_patient_encounter el
JOIN       tmp_nextgen_diagnosis_encounter e
ON         e.person_id=el.person_id
and		   e.enc_id=el.enc_id
JOIN       care_site l
ON         l.care_site_source_value=el.location_id
GROUP BY el.enc_nbr, l.care_site_id

END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE   PROCEDURE [dbo].[wz_src_nextgen_omop_person]
AS
  BEGIN
      IF EXISTS (SELECT 1
                 FROM   sysobjects
                 WHERE  xtype = 'u'
                        AND NAME = 'tmp_nextgen_race_status')
        DROP TABLE tmp_nextgen_race_status

      SELECT DISTINCT person_id,
                      race
      INTO   tmp_nextgen_race_status
      FROM   lz_src..lz_nextgen_person_race_xref hx
      WHERE  hx.race IS NOT NULL
             AND Rtrim(Ltrim(hx.race)) != ''
             AND hx.row_timestamp = (SELECT Max(row_timestamp)
                                     FROM   lz_src..lz_nextgen_person_race_xref t
                                     WHERE  t.person_id = hx.person_id)

      IF EXISTS (SELECT 1
                 FROM   sysobjects
                 WHERE  xtype = 'u'
                        AND NAME = 'tmp_nextgen_patient')
        DROP TABLE tmp_nextgen_patient

      SELECT s.person_id
      INTO   tmp_nextgen_patient
      FROM   lz_src..lz_nextgen_patient s,
             lz_src..lz_nextgen_patient_encounter e
      WHERE  s.person_id = e.person_id
      GROUP  BY s.person_id

      INSERT INTO person
                  (
                   gender_concept_id,
                   year_of_birth,
                   month_of_birth,
                   day_of_birth,
                   birth_datetime,
                   death_datetime,
                   race_concept_id,
                   ethnicity_concept_id,
                   location_id,
                   provider_id,
                   care_site_id,
                   person_source_value,
                   gender_source_value,
                   gender_source_concept_id,
                   race_source_value,
                   race_source_concept_id,
                   ethnicity_source_value,
                   ethnicity_source_concept_id)
      SELECT
             CASE Upper(a.sex)
               WHEN 'M' THEN 8507
               WHEN 'F' THEN 8532
			   ELSE 0
             END,
             Substring(a.date_of_birth, 1, 4),
             Substring(a.date_of_birth, 5, 2),
             Substring(a.date_of_birth, 7, 2),
             date_of_birth,
             CASE
               WHEN (a.expired_ind) = 'Y' THEN a.expired_date
             END               AS death_date,
             CASE lower(r.race)
               WHEN 'caucasian' THEN 8527
			   WHEN 'white' THEN 8527
               WHEN 'african american' THEN 8516
               WHEN 'black or african american' THEN 8516
               WHEN 'asian' THEN 8515
               ELSE 0
             END,
             CASE
               WHEN lower(a.ethnicity) = 'hispanic or latino' THEN 38003563
			   WHEN lower(a.ethnicity) = 'not hispanic or latino' THEN 38003564
               ELSE 0
             END,
             NULL,
             NULL,
             NULL,
             pt.med_rec_nbr,
             a.sex,
             0,
             a.race,
             0,
             a.ethnicity,
             0
      FROM   lz_src..lz_nextgen_person a
             JOIN lz_src..lz_nextgen_patient pt
               ON a.person_id = pt.person_id
             JOIN tmp_nextgen_patient e
               ON a.person_id = e.person_id
             LEFT JOIN tmp_nextgen_race_status r
                    ON r.person_id = a.person_id

  END  
  
  
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
--truncate table observation
--truncate table visit_detail




   CREATE   PROCEDURE [dbo].[wz_src_nextgen_omop_procedure]
AS
  BEGIN
    IF EXISTS
    (
           SELECT 1
           FROM   sysobjects
           WHERE  xtype='u'
           AND    NAME='tmp_nextgen_procedure_encounter')
    DROP TABLE tmp_nextgen_procedure_encounter
    SELECT DISTINCT p.person_id,
				    s.person_nbr,
                    i.enc_nbr,
                    p.enc_id
    INTO            tmp_nextgen_procedure_encounter
    FROM            lz_src..lz_nextgen_patient_procedure p
    JOIN            lz_src..lz_nextgen_patient_encounter i
    ON              i.enc_id=p.enc_id
    JOIN            lz_src..lz_nextgen_person s
    ON              s.person_id=p.person_id	
	

INSERT INTO observation
            (
                        observation_id,
                        person_id,
                        observation_concept_id,
                        observation_date,
                        observation_datetime,
                        observation_type_concept_id,
                        value_as_number,
                        value_as_string,
                        value_as_concept_id,
                        qualifier_concept_id,
                        unit_concept_id,
                        provider_id,
                        visit_occurrence_id,
                        visit_detail_id,
                        observation_source_value,
                        observation_source_concept_id,
                        unit_source_value,
                        qualifier_source_value,
						observation_event_id,
						obs_event_field_concept_id,
						value_as_datetime
            )
SELECT    
0,
           p.person_nbr AS patient_id,
		   0,
		   NULL,
           CASE
                      WHEN Isdate(NULLIF(a.service_date,'')) = 1 THEN a.service_date
                      ELSE GETDATE()
           END AS proc_date,
		   
            38000280,
           NULL, 
           null,
          null,
		   null,
           null,
           0, --provider_id
		   0,
	0,
	concat('CPT4:', a.cpt4_code_id), 
	'4',
	0,
	null,
	null,
            0,
           null
FROM       lz_src..lz_nextgen_patient_procedure a
JOIN       tmp_nextgen_procedure_encounter e
ON         e.person_id=a.person_id
AND        e.enc_id=a.enc_id
JOIN       lz_src..lz_nextgen_person p
ON         p.person_id=a.person_id

declare @counter int = 1;
declare @cpt varchar(75);
declare @sql nvarchar(4000);

while @counter < 12
begin
set  @cpt = concat('a.diagnosis_code_id_' , @counter);
print @cpt;
set @sql = N'INSERT INTO observation
            (
                        observation_id,
                        person_id,
                        observation_concept_id,
                        observation_date,
                        observation_datetime,
                        observation_type_concept_id,
                        value_as_number,
                        value_as_string,
                        value_as_concept_id,
                        qualifier_concept_id,
                        unit_concept_id,
                        provider_id,
                        visit_occurrence_id,
                        visit_detail_id,
                        observation_source_value,
                        observation_source_concept_id,
                        unit_source_value,
                        qualifier_source_value,
						observation_event_id,
						obs_event_field_concept_id,
						value_as_datetime
            )
SELECT    
0,
           p.person_nbr AS patient_id,
		   0,
		   NULL,
           CASE
                      WHEN Isdate(NULLIF(a.service_date,'''')) = 1 THEN a.service_date
                      ELSE GETDATE()
           END AS diag_date,
		   
            38000280,
           NULL, 
           null,
          null,
		   null,
           null,
           0, --provider_id
		   0,
	0,
	case when e9.icd_type = ''09'' then concat(''ICD9CM:'', e9.icd9cm_code_id)
	else  concat(''ICD'', e9.icd_type, ''CM:'', e9.icd9cm_code_id)
	 end,
	e9.icd_type,
	0,
	null,
	null,
            0,
           null
FROM       lz_src..lz_nextgen_patient_procedure a
JOIN       tmp_nextgen_procedure_encounter e
ON         e.person_id=a.person_id
AND        e.enc_id=a.enc_id
JOIN       lz_src..lz_nextgen_person p
ON         p.person_id=a.person_id
INNER JOIN lz_src..lz_nextgen_icd9cm_code_mstr e9
ON        ' + @cpt + ' = e9.icd9cm_code_id
where e9.icd9cm_code_id is not null
and ' + @cpt + ' is not null
and ' + @cpt + ' != '''' ';
execute sp_executesql @sql;

 set @counter = @counter + 1;
end

INSERT INTO visit_detail
            (
                        visit_detail_id ,
                        person_id ,
                        visit_detail_concept_id ,
                        visit_detail_start_date ,
                        visit_detail_start_datetime ,
                        visit_detail_end_date ,
                        visit_detail_end_datetime ,
                        visit_detail_type_concept_id ,
                        provider_id ,
                        care_site_id ,
                        discharge_to_concept_id ,
                        admitted_from_concept_id ,
                        admitted_from_source_value ,
                        visit_detail_source_value ,
                        visit_detail_source_concept_id ,
                        discharge_to_source_value ,
                        preceding_visit_detail_id ,
                        visit_detail_parent_id ,
                        visit_occurrence_id
            )
SELECT  
el.enc_nbr    AS encounter_id,
         Max(e.person_nbr) AS patient_id,
         0,
         NULL,
         Min(
         CASE
                  WHEN el.admit_date IS NOT NULL THEN el.admit_date
                  ELSE el.enc_timestamp
         END) AS encounter_start_date,
         NULL,
         Max(
         CASE
                  WHEN el.discharge_date IS NOT NULL THEN el.discharge_date
                  ELSE el.enc_timestamp
         END) AS encounter_end_date,
         0,
         1,
         l.care_site_id,
         0,
         3,
         4,
         NULL,
         0 ,
         NULL,
         NULL,
         NULL,
		 0
FROM     lz_src..lz_nextgen_patient_encounter el
JOIN       tmp_nextgen_procedure_encounter e
ON         e.person_id=el.person_id
and		   e.enc_id=el.enc_id
JOIN       care_site l
ON         l.care_site_source_value=el.location_id
GROUP BY el.enc_nbr, l.care_site_id

END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

  CREATE PROCEDURE [dbo].[wz_src_nextgen_omop_provider]
AS
  BEGIN
      INSERT INTO provider
                  (
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
      SELECT 
             Concat(a.first_name, ' ', a.last_name),
             a.national_provider_id,
             NULL,
			 0,
             0,
             NULL,
             0,
             a.provider_id,
             NULL,
             0,
            NULL,
             0          
      FROM   lz_src..provider_mstr a
  END  
 
GO
