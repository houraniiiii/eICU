-- Mechanical Circulatory Support extraction from eICU
-- Uses exact string matches from treatment, nursecharting, nurseassessment, and diagnosis tables
-- Emits one row per charted event with patientUnitStayID, mechanical_support_type, and chartOffset
-- Mechanical support categories captured: 
 --1-- ArtificialHeart
 --2-- LVAD
 --3-- RVAD
 --4-- BIVAD
 --5-- Impella
 --6-- IABP
 --7-- IABP_discontinued
 --8-- VAD
 --9-- ECMO
-- chartOffset preferentially uses the documented offset when present, otherwise the entry offset 

WITH treatment_events AS (
  SELECT
    patientUnitStayID,
    treatmentOffset AS chartOffset,
    CASE treatmentString
      WHEN 'cardiovascular|cardiac surgery|artificial heart implantation' THEN 'ArtificialHeart'
      WHEN 'cardiovascular|cardiac surgery|implantation of BIVAD' THEN 'BIVAD'
      WHEN 'cardiovascular|cardiac surgery|implantation of RVAD' THEN 'RVAD'
      WHEN 'cardiovascular|cardiac surgery|implantation of LVAD' THEN 'LVAD'
      WHEN 'cardiovascular|non-operative procedures|intraaortic balloon pump removal' THEN 'IABP_discontinued'
      WHEN 'cardiovascular|non-operative procedures|intraaortic balloon pump' THEN 'IABP'
      WHEN 'cardiovascular|shock|intraaortic balloon pump' THEN 'IABP'
    END AS mechanical_support_type
  FROM `physionet-data.eicu_crd.treatment`
  WHERE treatmentString IN (
    'cardiovascular|cardiac surgery|artificial heart implantation',
    'cardiovascular|cardiac surgery|implantation of BIVAD',
    'cardiovascular|cardiac surgery|implantation of RVAD',
    'cardiovascular|cardiac surgery|implantation of LVAD',
    'cardiovascular|non-operative procedures|intraaortic balloon pump removal',
    'cardiovascular|non-operative procedures|intraaortic balloon pump',
    'cardiovascular|shock|intraaortic balloon pump'
  )
),
nursecharting_events AS (
  SELECT
    patientUnitStayID,
    COALESCE(nursingChartOffset, nursingChartEntryOffset) AS chartOffset,
    CASE nursingChartCellTypeValLabel
      WHEN 'LVAD' THEN 'LVAD'
      WHEN 'Impella' THEN 'Impella'
      WHEN 'ECMO' THEN 'ECMO'
    END AS mechanical_support_type
  FROM `physionet-data.eicu_crd.nursecharting`
  WHERE nursingChartCellTypeValLabel IN ('LVAD', 'Impella', 'ECMO')
),
nurseassessment_source AS (
  SELECT
    patientUnitStayID,
    COALESCE(nurseAssessOffset, nurseAssessEntryOffset) AS chartOffset,
    cellAttributeValue
  FROM `physionet-data.eicu_crd.nurseassessment`
  WHERE cellLabel = 'Ventricular Assist Device'
    AND cellAttributeValue IS NOT NULL
),
nurseassessment_events AS (
  SELECT
    patientUnitStayID,
    chartOffset,
    'Impella' AS mechanical_support_type
  FROM nurseassessment_source
  WHERE cellAttributeValue IN (
    'Impella',
    'IMPELLA',
    'Impella, IABP',
    'P8/purge flow 11.1',
    'P8, 1:1',
    'P7/purge flow 10.6',
    'P2',
    'P-8'
  )
  UNION ALL
  SELECT
    patientUnitStayID,
    chartOffset,
    'IABP' AS mechanical_support_type
  FROM nurseassessment_source
  WHERE cellAttributeValue IN (
    'IABP',
    'iabp',
    'Impella, IABP',
    '01:01',
    '1 to 1',
    'P8, 1:1'
  )
  UNION ALL
  SELECT
    patientUnitStayID,
    chartOffset,
    'LVAD' AS mechanical_support_type
  FROM nurseassessment_source
  WHERE cellAttributeValue IN (
    'Thoratec HeartMate II,  Software Version 4.16',
    'Heartmate II ',
    'Fixed mode spped 8600-9000',
    'Fixed mode',
    'Fixed'
  )
  UNION ALL
  SELECT
    patientUnitStayID,
    chartOffset,
    'VAD' AS mechanical_support_type
  FROM nurseassessment_source
  WHERE cellAttributeValue IN (
    'Thoratech',
    'Yes'
  )
),
diagnosis_events AS (
  SELECT
    patientUnitStayID,
    diagnosisOffset AS chartOffset,
    CASE diagnosisString
      WHEN 'cardiovascular|cardiac surgery|s/p LVAD' THEN 'LVAD'
      WHEN 'cardiovascular|cardiac surgery|s/p RVAD' THEN 'RVAD'
      WHEN 'cardiovascular|cardiac surgery|s/p BIVAD' THEN 'BIVAD'
      WHEN 'surgery|cardiac surgery|low cardiac output state|IABP' THEN 'IABP'
    END AS mechanical_support_type
  FROM `physionet-data.eicu_crd.diagnosis`
  WHERE diagnosisString IN (
    'cardiovascular|cardiac surgery|s/p LVAD',
    'cardiovascular|cardiac surgery|s/p RVAD',
    'cardiovascular|cardiac surgery|s/p BIVAD',
    'surgery|cardiac surgery|low cardiac output state|IABP'
  )
),
all_events AS (
  SELECT * FROM treatment_events
  UNION ALL
  SELECT * FROM nursecharting_events
  UNION ALL
  SELECT * FROM nurseassessment_events
  UNION ALL
  SELECT * FROM diagnosis_events
)
SELECT
  patientUnitStayID,
  mechanical_support_type,
  chartOffset
FROM all_events;
