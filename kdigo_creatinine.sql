-- KDIGO creatinine series for eICU (BigQuery)
-- Output columns: patientunitstayid, patienthealthsystemstayid, labresultoffset, creat, creat_low_past_48hr, creat_low_past_7day


WITH
raw_creatinine AS (
  SELECT
    p.patientUnitStayID,
    p.patientHealthSystemStayID,
    l.labResultOffset,
    l.labResult AS creat
  FROM `physionet-data.eicu_crd.patient` AS p
  JOIN `physionet-data.eicu_crd.lab` AS l
    ON l.patientUnitStayID = p.patientUnitStayID
  WHERE
    LOWER(l.labName) = 'creatinine'
    AND l.labResult IS NOT NULL
    AND l.labResult BETWEEN 0.01 AND 150
    AND l.labResultOffset >= -10080
    AND (p.unitDischargeOffset IS NULL OR l.labResultOffset <= p.unitDischargeOffset)
),

creatinine_series AS (
  SELECT
    patientUnitStayID,
    patientHealthSystemStayID,
    labResultOffset,
    AVG(creat) AS creat
  FROM raw_creatinine
  GROUP BY patientUnitStayID, patientHealthSystemStayID, labResultOffset
),

creatinine_48h AS (
  SELECT
    cs.patientUnitStayID,
    cs.labResultOffset,
    MIN(prev.creat) AS creat_low_past_48hr
  FROM creatinine_series AS cs
  LEFT JOIN creatinine_series AS prev
    ON cs.patientUnitStayID = prev.patientUnitStayID
   AND prev.labResultOffset < cs.labResultOffset
   AND prev.labResultOffset >= cs.labResultOffset - 2880
  GROUP BY cs.patientUnitStayID, cs.labResultOffset
),

creatinine_7day AS (
  SELECT
    cs.patientUnitStayID,
    cs.labResultOffset,
    MIN(prev.creat) AS creat_low_past_7day
  FROM creatinine_series AS cs
  LEFT JOIN creatinine_series AS prev
    ON cs.patientUnitStayID = prev.patientUnitStayID
   AND prev.labResultOffset < cs.labResultOffset
   AND prev.labResultOffset >= cs.labResultOffset - 10080
  GROUP BY cs.patientUnitStayID, cs.labResultOffset
)

SELECT
  cs.patientUnitStayID AS patientunitstayid,
  cs.patientHealthSystemStayID AS patienthealthsystemstayid,
  cs.labResultOffset AS labresultoffset,
  cs.creat,
  c48.creat_low_past_48hr,
  c7.creat_low_past_7day
FROM creatinine_series AS cs
LEFT JOIN creatinine_48h AS c48
  ON cs.patientUnitStayID = c48.patientUnitStayID
 AND cs.labResultOffset = c48.labResultOffset
LEFT JOIN creatinine_7day AS c7
  ON cs.patientUnitStayID = c7.patientUnitStayID
 AND cs.labResultOffset = c7.labResultOffset
ORDER BY patientunitstayid, labresultoffset;
