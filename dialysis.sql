-- Dialysis events extraction from eICU (BigQuery)
-- Output columns: patientunitstayid, chartoffset (min from ICU admit), dialysis_type
-- Categories emitted exactly as specified:
--   SCUF
--   SLED
--   CVVH
--   CAVHD
--   CVVHD
--   Intermittent hemodialysis (IHD)
--   peritoneal dialysis,
--   av_fistula
--   av_shunt
--   hemodialysis_catheter
--   peritoneal_catheter
--   dialysis_electrolytes
--   dialysis_output
--   past hemodialysis
--   past peritoneal dialysis
--   dialysis_graft

---- chartoffset preferentially uses the documented offset when present, otherwise the entry offset 


-- 1) Treatment events: exact treatmentString mappings to modality categories
WITH treatment_events AS (
  SELECT
    patientUnitStayID,
    treatmentOffset AS chartoffset,
    CASE treatmentString
      -- SCUF
      WHEN 'renal|dialysis|ultrafiltration (fluid removal only)' THEN 'SCUF'
      WHEN 'renal|dialysis|ultrafiltration (fluid removal only)|emergent' THEN 'SCUF'
      WHEN 'renal|dialysis|ultrafiltration (fluid removal only)|for acute renal failure' THEN 'SCUF'
      WHEN 'renal|dialysis|ultrafiltration (fluid removal only)|for chronic renal failure' THEN 'SCUF'
      -- SLED
      WHEN 'renal|dialysis|SLED' THEN 'SLED'
      -- CVVH / CAVHD / CVVHD
      WHEN 'renal|dialysis|C V V H' THEN 'CVVH'
      WHEN 'renal|dialysis|C A V H D' THEN 'CAVHD'
      WHEN 'renal|dialysis|C V V H D' THEN 'CVVHD'
      -- Intermittent hemodialysis (IHD)
      WHEN 'renal|dialysis|hemodialysis' THEN 'Intermittent hemodialysis (IHD)'
      WHEN 'renal|dialysis|hemodialysis|emergent' THEN 'Intermittent hemodialysis (IHD)'
      WHEN 'renal|dialysis|hemodialysis|for acute renal failure' THEN 'Intermittent hemodialysis (IHD)'
      WHEN 'renal|dialysis|hemodialysis|for chronic renal failure' THEN 'Intermittent hemodialysis (IHD)'
      WHEN 'toxicology|drug overdose|drug removal measures|hemodialysis' THEN 'Intermittent hemodialysis (IHD)'
      -- peritoneal dialysis
      WHEN 'renal|dialysis|peritoneal dialysis' THEN 'peritoneal dialysis'
      WHEN 'renal|dialysis|peritoneal dialysis|emergent' THEN 'peritoneal dialysis'
      WHEN 'renal|dialysis|peritoneal dialysis|for chronic renal failure' THEN 'peritoneal dialysis'
      WHEN 'renal|dialysis|peritoneal dialysis|for acute renal failure' THEN 'peritoneal dialysis'
      WHEN 'renal|dialysis|peritoneal dialysis|with cannula placement' THEN 'peritoneal dialysis'
      -- Access, catheter, and electrolyte-related dialysis entries
      WHEN 'renal|dialysis|arteriovenous shunt for renal dialysis' THEN 'av_shunt'
      WHEN 'cardiovascular|vascular surgery|dialysis access surgery' THEN 'av_fistula'
      WHEN 'renal|dialysis|insertion of venous catheter for hemodialysis' THEN 'hemodialysis_catheter'
      WHEN 'renal|dialysis|insertion of venous catheter for hemodialysis|tunneled catheter' THEN 'hemodialysis_catheter'
      WHEN 'renal|dialysis|insertion of venous catheter for hemodialysis|percutaneous catheter' THEN 'hemodialysis_catheter'
      WHEN 'renal|procedures/radiology|arteriovenous shunt for renal dialysis' THEN 'av_shunt'
      WHEN 'renal|procedures/radiology|insertion of catheter for peritoneal dialysis' THEN 'peritoneal_catheter'
      WHEN 'renal|procedures/radiology|insertion of venous catheter for hemodialysis' THEN 'hemodialysis_catheter'
      WHEN 'renal|procedures/radiology|insertion of venous catheter for hemodialysis|tunneled catheter' THEN 'hemodialysis_catheter'
      WHEN 'renal|procedures/radiology|insertion of venous catheter for hemodialysis|percutaneous catheter' THEN 'hemodialysis_catheter'
      WHEN 'endocrine|electrolyte correction|treatment of hyperkalemia|dialysis' THEN 'dialysis_electrolytes'
      WHEN 'renal|electrolyte correction|treatment of hyperkalemia|dialysis' THEN 'dialysis_electrolytes'
      WHEN 'renal|electrolyte correction|treatment of hyperphosphatemia|dialysis' THEN 'dialysis_electrolytes'
    END AS dialysis_type
  FROM `physionet-data.eicu_crd.treatment`
  WHERE treatmentString IN (
    'renal|dialysis|SLED',
    'renal|dialysis|C V V H',
    'renal|dialysis|C A V H D',
    'renal|dialysis|C V V H D',
    'renal|dialysis|hemodialysis',
    'renal|dialysis|peritoneal dialysis',
    'renal|dialysis|hemodialysis|emergent',
    'renal|dialysis|peritoneal dialysis|emergent',
    'renal|dialysis|hemodialysis|for acute renal failure',
    'renal|dialysis|ultrafiltration (fluid removal only)',
    'renal|dialysis|arteriovenous shunt for renal dialysis',
    'renal|dialysis|hemodialysis|for chronic renal failure',
    'cardiovascular|vascular surgery|dialysis access surgery',
    'toxicology|drug overdose|drug removal measures|hemodialysis',
    'renal|dialysis|insertion of venous catheter for hemodialysis',
    'renal|dialysis|peritoneal dialysis|for chronic renal failure',
    'renal|dialysis|ultrafiltration (fluid removal only)|emergent',
    'renal|electrolyte correction|treatment of hyperkalemia|dialysis',
    'renal|procedures/radiology|arteriovenous shunt for renal dialysis',
    'endocrine|electrolyte correction|treatment of hyperkalemia|dialysis',
    'renal|electrolyte correction|treatment of hyperphosphatemia|dialysis',
    'renal|procedures/radiology|insertion of catheter for peritoneal dialysis',
    'renal|procedures/radiology|insertion of venous catheter for hemodialysis',
    'renal|dialysis|ultrafiltration (fluid removal only)|for acute renal failure',
    'renal|dialysis|ultrafiltration (fluid removal only)|for chronic renal failure',
    'renal|dialysis|insertion of venous catheter for hemodialysis|tunneled catheter',
    'renal|dialysis|insertion of venous catheter for hemodialysis|percutaneous catheter',
    'renal|procedures/radiology|insertion of venous catheter for hemodialysis|tunneled catheter',
    'renal|procedures/radiology|insertion of venous catheter for hemodialysis|percutaneous catheter',
    'renal|dialysis|peritoneal dialysis|with cannula placement',
    'renal|dialysis|peritoneal dialysis|for acute renal failure'
  )
),

-- 2) Nurse assessment: AV Fistula presence indicates a dialysis-related access event
nurseassessment_events AS (
  SELECT
    patientUnitStayID,
    COALESCE(nurseAssessOffset, nurseAssessEntryOffset) AS chartoffset,
    'av_fistula' AS dialysis_type
  FROM `physionet-data.eicu_crd.nurseassessment`
  WHERE cellLabel = 'AV Fistula'
),

-- 3) Intake/Output: dialysisTotal indicates a dialysis session; dedupe on (stay, offset)
intakeoutput_raw AS (
  SELECT
    patientUnitStayID,
    intakeOutputOffset,
    intakeOutputEntryOffset,
    dialysisTotal
  FROM `physionet-data.eicu_crd.intakeoutput`
  WHERE dialysisTotal IS NOT NULL AND dialysisTotal != 0
),
intakeoutput_dedup AS (
  SELECT DISTINCT
    patientUnitStayID,
    intakeOutputOffset,
    intakeOutputEntryOffset
  FROM intakeoutput_raw
),
intakeoutput_events AS (
  SELECT
    patientUnitStayID,
    COALESCE(intakeOutputOffset, intakeOutputEntryOffset) AS chartoffset,
    'dialysis_output' AS dialysis_type
  FROM intakeoutput_dedup
),

-- 4) Diagnosis: exact diagnosisString mappings
diagnosis_events AS (
  SELECT
    patientUnitStayID,
    diagnosisOffset AS chartoffset,
    CASE diagnosisString
      WHEN 'cardiovascular|post vascular surgery|s/p dialysis access surgery' THEN 'av_fistula'
      WHEN 'renal|disorder of kidney|acute renal failure|unstable during hemodialysis' THEN 'Intermittent hemodialysis (IHD)'
    END AS dialysis_type
  FROM `physionet-data.eicu_crd.diagnosis`
  WHERE diagnosisString IN (
    'cardiovascular|post vascular surgery|s/p dialysis access surgery',
    'renal|disorder of kidney|acute renal failure|unstable during hemodialysis'
  )
),

-- 5) Past history: chronic modality identification
pasthistory_events AS (
  SELECT
    patientUnitStayID,
    COALESCE(pastHistoryOffset, pastHistoryEnteredOffset) AS chartoffset,
    CASE pastHistoryPath
      WHEN 'notes/Progress Notes/Past History/Organ Systems/Renal  (R)/Renal Failure/renal failure - hemodialysis' THEN 'past hemodialysis'
      WHEN 'notes/Progress Notes/Past History/Organ Systems/Renal  (R)/Renal Failure/renal failure - peritoneal dialysis' THEN 'past peritoneal dialysis'
    END AS dialysis_type
  FROM `physionet-data.eicu_crd.pasthistory`
  WHERE pastHistoryPath IN (
    'notes/Progress Notes/Past History/Organ Systems/Renal  (R)/Renal Failure/renal failure - hemodialysis',
    'notes/Progress Notes/Past History/Organ Systems/Renal  (R)/Renal Failure/renal failure - peritoneal dialysis'
  )
),

-- 6) Admission diagnosis: access-related surgery at admission
admissiondx_events AS (
  SELECT
    patientUnitStayID,
    admitDxEnteredOffset AS chartoffset,
    'dialysis_graft' AS dialysis_type
  FROM `physionet-data.eicu_crd.admissiondx`
  WHERE admitDxPath = 'admission diagnosis|All Diagnosis|Operative|Diagnosis|Cardiovascular|Graft for dialysis, insertion of'
),

-- Final union of all event feeds
all_events AS (
  SELECT * FROM treatment_events
  UNION ALL
  SELECT * FROM nurseassessment_events
  UNION ALL
  SELECT * FROM intakeoutput_events
  UNION ALL
  SELECT * FROM diagnosis_events
  UNION ALL
  SELECT * FROM pasthistory_events
  UNION ALL
  SELECT * FROM admissiondx_events
)
SELECT
  patientunitstayid,
  chartoffset,
  dialysis_type
FROM all_events;



