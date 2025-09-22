WITH patient_admission_weights AS (
    SELECT
        p.patientunitstayid
        , COALESCE(p.unitadmitoffset, 0) AS chartoffset
        , CAST(p.admissionweight AS DOUBLE PRECISION) AS weight
        , 'admit' AS weight_type
    FROM patient p
    WHERE p.admissionweight BETWEEN 20 AND 1000
)

, patient_discharge_weights AS (
    SELECT
        p.patientunitstayid
        , p.unitdischargeoffset AS chartoffset
        , CAST(p.dischargeweight AS DOUBLE PRECISION) AS weight
        , 'daily' AS weight_type
    FROM patient p
    WHERE p.dischargeweight BETWEEN 20 AND 1000
        AND p.unitdischargeoffset IS NOT NULL
)

, infusion_weights AS (
    SELECT
        i.patientunitstayid
        , i.drugstartoffset AS chartoffset
        , CAST(i.patientweight AS DOUBLE PRECISION) AS weight
        , 'daily' AS weight_type
    FROM infusiondrug i
    WHERE i.patientweight BETWEEN 20 AND 1000
        AND i.drugstartoffset IS NOT NULL
)

, physical_exam_weights AS (
    SELECT
        pe.patientunitstayid
        , pe.physicalexamoffset AS chartoffset
        , CAST(
            NULLIF(
                (regexp_match(pe.physicalexamtext, '([0-9]+(?:\.[0-9]+)?)'))[1]
                , ''
            ) AS DOUBLE PRECISION
        ) AS weight
        , CASE
            WHEN pe.physicalexampath = 'notes/Progress Notes/Physical Exam/Physical Exam/Constitutional/Weight and I&O/Weight (kg)/Admission'
                THEN 'admit'
            ELSE 'daily'
        END AS weight_type
    FROM physicalexam pe
    WHERE pe.physicalexampath IN (
        'notes/Progress Notes/Physical Exam/Physical Exam/Constitutional/Weight and I&O/Weight (kg)/Current'
        , 'notes/Progress Notes/Physical Exam/Physical Exam/Constitutional/Weight and I&O/Weight (kg)/Admission'
    )
        AND pe.physicalexamtext IS NOT NULL
        AND pe.physicalexamoffset IS NOT NULL
)

, intake_output_weights AS (
    SELECT
        io.patientunitstayid
        , io.intakeoutputoffset AS chartoffset
        , CASE
            WHEN UPPER(io.celllabel) = 'BODYWEIGHT (LB)'
                THEN io.cellvaluenumeric * 0.45359237
            ELSE io.cellvaluenumeric
        END AS weight
        , 'daily' AS weight_type
    FROM intakeoutput io
    WHERE UPPER(io.celllabel) IN ('BODYWEIGHT (KG)', 'BODYWEIGHT (LB)')
        AND io.cellvaluenumeric IS NOT NULL
        AND io.intakeoutputoffset IS NOT NULL
)

, all_weights AS (
    SELECT * FROM patient_admission_weights
    UNION ALL
    SELECT * FROM patient_discharge_weights
    UNION ALL
    SELECT * FROM infusion_weights
    UNION ALL
    SELECT * FROM physical_exam_weights
    UNION ALL
    SELECT * FROM intake_output_weights
)

, filtered_weights AS (
    SELECT
        aw.patientunitstayid
        , aw.chartoffset
        , aw.weight
        , CASE
            WHEN aw.weight_type = 'admit' THEN 'admit'
            ELSE 'daily'
        END AS weight_type
    FROM all_weights aw
    WHERE aw.weight BETWEEN 20 AND 1000
        AND aw.chartoffset IS NOT NULL
)

, wt_stg1 AS (
    SELECT
        fw.patientunitstayid
        , fw.chartoffset
        , fw.weight_type
        , fw.weight
        , ROW_NUMBER() OVER (
            PARTITION BY fw.patientunitstayid, fw.weight_type
            ORDER BY fw.chartoffset
        ) AS rn
    FROM filtered_weights fw
)

, wt_stg2 AS (
    SELECT
        wt_stg1.patientunitstayid
        , COALESCE(p.unitadmitoffset, 0) AS unitadmitoffset
        , p.unitdischargeoffset
        , wt_stg1.weight_type
        , CASE
            WHEN wt_stg1.weight_type = 'admit' AND wt_stg1.rn = 1
                THEN COALESCE(p.unitadmitoffset, 0) - 120
            ELSE wt_stg1.chartoffset
        END AS startoffset
        , wt_stg1.weight
    FROM wt_stg1
    INNER JOIN patient p
        ON p.patientunitstayid = wt_stg1.patientunitstayid
)

, wt_stg3 AS (
    SELECT
        wt_stg2.patientunitstayid
        , wt_stg2.startoffset
        , COALESCE(
            LEAD(wt_stg2.startoffset) OVER (
                PARTITION BY wt_stg2.patientunitstayid
                ORDER BY wt_stg2.startoffset
            )
            , COALESCE(wt_stg2.unitdischargeoffset, wt_stg2.startoffset) + 120
        ) AS endoffset
        , wt_stg2.weight
        , wt_stg2.weight_type
        , wt_stg2.unitadmitoffset
        , wt_stg2.unitdischargeoffset
    FROM wt_stg2
)

, wt1 AS (
    SELECT
        wt_stg3.patientunitstayid
        , wt_stg3.startoffset
        , COALESCE(
            wt_stg3.endoffset
            , LEAD(wt_stg3.startoffset) OVER (
                PARTITION BY wt_stg3.patientunitstayid
                ORDER BY wt_stg3.startoffset
            )
            , COALESCE(wt_stg3.unitdischargeoffset, wt_stg3.startoffset) + 120
        ) AS endoffset
        , wt_stg3.weight
        , wt_stg3.weight_type
    FROM wt_stg3
)

, wt_fix AS (
    SELECT
        p.patientunitstayid
        , COALESCE(p.unitadmitoffset, 0) - 120 AS startoffset
        , wt.startoffset AS endoffset
        , wt.weight
        , wt.weight_type
    FROM patient p
    INNER JOIN (
        SELECT
            wt1.patientunitstayid
            , wt1.startoffset
            , wt1.weight
            , wt1.weight_type
            , ROW_NUMBER() OVER (
                PARTITION BY wt1.patientunitstayid
                ORDER BY wt1.startoffset
            ) AS rn
        FROM wt1
    ) wt
        ON wt.patientunitstayid = p.patientunitstayid
        AND wt.rn = 1
    WHERE COALESCE(p.unitadmitoffset, 0) < wt.startoffset
)

SELECT
    patientunitstayid
    , startoffset
    , endoffset
    , weight
    , weight_type
FROM wt1
UNION ALL
SELECT
    patientunitstayid
    , startoffset
    , endoffset
    , weight
    , weight_type
FROM wt_fix
ORDER BY patientunitstayid, startoffset;
