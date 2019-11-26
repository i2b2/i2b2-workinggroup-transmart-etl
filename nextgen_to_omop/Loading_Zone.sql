


CREATE PROCEDURE [dbo].[Lz_src_nextgen_omop_person]
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
      FROM   lz_src..person_race_xref hx
      WHERE  hx.race IS NOT NULL
             AND Rtrim(Ltrim(hx.race)) != ''
             AND hx.row_timestamp = (SELECT Max(row_timestamp)
                                     FROM   lz_src..person_race_xref t
                                     WHERE  t.person_id = hx.person_id)

      IF EXISTS (SELECT 1
                 FROM   sysobjects
                 WHERE  xtype = 'u'
                        AND NAME = 'tmp_nextgen_patient')
        DROP TABLE tmp_nextgen_patient

      SELECT s.person_id
      INTO   tmp_nextgen_patient
      FROM   lz_src..patient s,
             lz_src..patient_encounter e
      WHERE  s.person_id = e.person_id
      GROUP  BY s.person_id

      IF EXISTS (SELECT 1
                 FROM   sysobjects
                 WHERE  xtype = 'u'
                        AND NAME = 'tmp_nextgen_patient_death')
        DROP TABLE tmp_nextgen_patient_death

      SELECT p.person_id,
             p.expired_date
      INTO   tmp_nextgen_patient_death
      FROM   lz_src..person p
      WHERE  expired_ind = 'Y'

      INSERT INTO person
                  (person_id,
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
      SELECT a.other_id_number AS mrn,
             CASE Upper(a.sex)
               WHEN 'M' THEN 8507
               WHEN 'F' THEN 8532
             END,
             Substring(a.date_of_birth, 1, 4),
             Substring(a.date_of_birth, 4, 2),
             Substring(a.date_of_birth, 6, 2),
             date_of_birth,
             CASE
               WHEN Isdate(d.expired_date) = 1 THEN d.expired_date
             END               AS death_date,
             CASE Upper(r.race)
               WHEN 'WHITE' THEN 8527
               WHEN 'BLACK' THEN 8516
               WHEN 'ASIAN' THEN 8515
               ELSE 0
             END,
             CASE
               WHEN Upper(a.ethnicity) = 'HISPANIC' THEN 38003563
               ELSE 0
             END,
             NULL,
             NULL,
             NULL,
             a.other_id_number,
             a.sex,
             0,
             a.race,
             0,
             a.ethnicity,
             0
      FROM   lz_src..person a
             JOIN lz_src..patient pt
               ON a.person_id = pt.person_id
             JOIN tmp_nextgen_patient e
               ON a.person_id = e.person_id
             LEFT JOIN tmp_nextgen_patient_death d
                    ON d.person_id = a.person_id
             LEFT JOIN tmp_nextgen_race_status r
                    ON r.person_id = a.person_id
  END  
  
  
   CREATE PROCEDURE [dbo].[Lz_src_nextgen_omop_diagnosis]
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
                    a.appt_encounter_number,
                    p.enc_id
    INTO            xxx_nextgen_diagnosis_encounter
    FROM            lz_src..patient_diagnosis p
    JOIN            tmp_nextgen_patient_include i
    ON              i.person_id=p.person_id)
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
                        qualifier_source_value
            )
SELECT     Nextval('observation_id_seq'),
           a.person_id AS patient_id,
           srctostdvm.target_concept_id,
           NULL,
           CASE
                      WHEN Isdate(NULLIF(a.create_timestamp,'')) = 1 THEN a.create_timestamp
                      ELSE NULL
           END AS diag_date,
           rd     38000280,
           NULL,
           NULL,
           0,
           0,
           0,
           0,
           (
                  SELECT fv.visit_occurrence_id_new
                  FROM   final_visit_ids fv
                  WHERE  fv.encounter_id = a.encounter) visit_occurrence_id,
           0,
           a.code,
           (
                  SELECT srctosrcvm.source_concept_id
                  FROM   source_to_source_vocab_map srctosrcvm
                  WHERE  srctosrcvm.source_code = a.code
                  AND    srctosrcvm.source_vocabulary_id = 'ICD' )
FROM       lz_src..patient_diagnosis a
JOIN       xxx_nextgen_diagnosis_encounter e
ON         e.person_id=a.person_id
AND        e.enc_id=a.enc_id
LEFT JOIN  lz_src..location_mstr l
ON         a.location_id=l.location_id
LEFT JOIN  lz_src..diagnosis_severity_mstr dsm
ON         a.severity_id=dsm.severity_id
LEFT JOIN  lz_src..diagnosis_status_mstr sm
ON         a.status_id=sm.status_id
INNER JOIN lz_src..icd9cm_code_mstr e9
ON         a.icd9cm_code_id=e9.icd9cm_code_id
JOIN       tmp_nextgen_diagnosis_sequence s
ON         s.person_id=a.person_id
AND        s.enc_id=a.enc_id
JOIN       source_to_standard_vocab_map srctostdvm
ON         srctostdvm.source_code = a.code
AND        srctostdvm.target_domain_id = 'Observation'
AND        srctostdvm.target_vocabulary_id = 'ICD'
AND        srctostdvm.target_standard_concept = 'S'
AND        srctostdvm.target_invalid_reason IS NULL
INSERT INTO dbo.visit_detail
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
SELECT   el.encounter_id    AS encounter_id,
         Max(el.patient_id) AS patient_id,
         0,
         NULL,
         Min(
         CASE
                  WHEN s.enc_date IS NOT NULL THEN s.enc_date
                  ELSE t.enc_start_date
         END) AS encounter_start_date,
         NULL,
         Max(
         CASE
                  WHEN s.enc_date IS NOT NULL THEN s.enc_date
                  ELSE t.enc_end_date
         END) AS encounter_end_date,
         0,
         NULL,
         NULL,
         NULL,
         NULL,
         NULL,
         NULL,
         0 null,
         NULL,
         NULL,
         NULL
FROM     lz.src..diagnosis el
JOIN
         (
                  SELECT   patient_id,
                           encounter_id,
                           Min(diag_date) AS enc_start_date,
                           Max(diag_date) AS enc_end_date
                  FROM     lz_std_enc_diagnosis
                  GROUP BY patient_id,
                           encounter_id) t
ON       t.patient_id=el.patient_id
AND      t.encounter_id=el.encounter_id
WHERE    el.encounter_id_type='NEXTGEN'
AND      NOT EXISTS
         (
                SELECT 1
                FROM   lz_src..encounter enc
                WHERE  enc.encounter_id=el.encounter_id
                AND    enc.encounter_id_type=el.encounter_id_type )
GROUP BY el.encounter_id

END


 CREATE PROCEDURE [dbo].[Lz_src_nextgen_omop_provider]
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
             Concat(a.firest_name, ' ', a.last_name),
             a.national_provider_id,
             NULL,
             0,
             NULL,
             NULL,
             0,
             NULL,
             NULL,
             0,
             NULL          0
      FROM   lz_src..provider_mstr a
  END  
 