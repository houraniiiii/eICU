WITH treatment_events AS (
  SELECT
    patientUnitStayID,
    treatmentOffset AS chartoffset,
    CASE treatmentString
      -- SCUF
      WHEN 'renal|dialysis|ultrafiltration (fluid removal only)' THEN 'unknown_SCUF'
      WHEN 'renal|dialysis|ultrafiltration (fluid removal only)|emergent' THEN 'acute_SCUF'
      WHEN 'renal|dialysis|ultrafiltration (fluid removal only)|for acute renal failure' THEN 'acute_SCUF'
      WHEN 'renal|dialysis|ultrafiltration (fluid removal only)|for chronic renal failure' THEN 'chronic_SCUF'
      -- SLED
      WHEN 'renal|dialysis|SLED' THEN 'SLED'
      -- CVVH / CAVHD / CVVHD
      WHEN 'renal|dialysis|C V V H' THEN 'CVVH'
      WHEN 'renal|dialysis|C A V H D' THEN 'CAVHD'
      WHEN 'renal|dialysis|C V V H D' THEN 'CVVHD'
      -- intermittent_hemodialysis
      WHEN 'renal|dialysis|hemodialysis' THEN 'unknown_intermittent_hemodialysis'
      WHEN 'renal|dialysis|hemodialysis|emergent' THEN 'acute_intermittent_hemodialysis'
      WHEN 'renal|dialysis|hemodialysis|for acute renal failure' THEN 'acute_intermittent_hemodialysis'
      WHEN 'renal|dialysis|hemodialysis|for chronic renal failure' THEN 'chronic_intermittent_hemodialysis'
      WHEN 'toxicology|drug overdose|drug removal measures|hemodialysis' THEN 'toxicology_intermittent_hemodialysis'
      -- peritoneal dialysis
      WHEN 'renal|dialysis|peritoneal dialysis' THEN 'unknown_peritoneal dialysis'
      WHEN 'renal|dialysis|peritoneal dialysis|emergent' THEN 'acute_peritoneal dialysis'
      WHEN 'renal|dialysis|peritoneal dialysis|for chronic renal failure' THEN 'chronic_peritoneal dialysis'
      WHEN 'renal|dialysis|peritoneal dialysis|for acute renal failure' THEN 'acute_peritoneal dialysis'
      WHEN 'renal|dialysis|peritoneal dialysis|with cannula placement' THEN 'unknown_peritoneal dialysis'
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

nurseassessment_events AS (
  SELECT
    patientUnitStayID,
    COALESCE(nurseAssessOffset, nurseAssessEntryOffset) AS chartoffset,
    'av_fistula' AS dialysis_type
  FROM `physionet-data.eicu_crd.nurseassessment`
  WHERE cellLabel = 'AV Fistula'
),

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

diagnosis_events AS (
  SELECT
    patientUnitStayID,
    diagnosisOffset AS chartoffset,
    CASE diagnosisString
      WHEN 'cardiovascular|post vascular surgery|s/p dialysis access surgery' THEN 'av_fistula'
    END AS dialysis_type
  FROM `physionet-data.eicu_crd.diagnosis`
  WHERE diagnosisString IN (
    'cardiovascular|post vascular surgery|s/p dialysis access surgery',
    'renal|disorder of kidney|acute renal failure|unstable during hemodialysis'
  )
),

pasthistory_events AS (
  SELECT
    patientUnitStayID,
    COALESCE(pastHistoryOffset, pastHistoryEnteredOffset) AS chartoffset,
    CASE pastHistoryPath
      WHEN 'notes/Progress Notes/Past History/Organ Systems/Renal  (R)/Renal Failure/renal failure - hemodialysis' THEN 'past_hemodialysis'
      WHEN 'notes/Progress Notes/Past History/Organ Systems/Renal  (R)/Renal Failure/renal failure - peritoneal dialysis' THEN 'past_peritoneal_dialysis'
    END AS dialysis_type
  FROM `physionet-data.eicu_crd.pasthistory`
  WHERE pastHistoryPath IN (
    'notes/Progress Notes/Past History/Organ Systems/Renal  (R)/Renal Failure/renal failure - hemodialysis',
    'notes/Progress Notes/Past History/Organ Systems/Renal  (R)/Renal Failure/renal failure - peritoneal dialysis'
  )
),

admissiondx_events AS (
  SELECT
    patientUnitStayID,
    admitDxEnteredOffset AS chartoffset,
    'dialysis_graft' AS dialysis_type
  FROM `physionet-data.eicu_crd.admissiondx`
  WHERE admitDxPath = 'admission diagnosis|All Diagnosis|Operative|Diagnosis|Cardiovascular|Graft for dialysis, insertion of'
),

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
