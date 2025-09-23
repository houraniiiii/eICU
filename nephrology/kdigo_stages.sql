WITH cr_stg AS (
    SELECT
        cr.patientunitstayid
        , cr.patienthealthsystemstayid
        , cr.labresultoffset AS chartoffset
        , cr.creat_low_past_7day
        , cr.creat_low_past_48hr
        , cr.creat
        , CASE
            WHEN cr.creat IS NULL THEN NULL
            WHEN cr.creat_low_past_7day IS NULL THEN NULL
            WHEN cr.creat >= (cr.creat_low_past_7day * 3.0) THEN 3
            WHEN cr.creat >= 4
                AND (
                    cr.creat_low_past_48hr <= 3.7
                    OR cr.creat >= (1.5 * cr.creat_low_past_7day)
                ) THEN 3
            WHEN cr.creat >= (cr.creat_low_past_7day * 2.0) THEN 2
            WHEN cr.creat_low_past_48hr IS NOT NULL
                AND cr.creat >= (cr.creat_low_past_48hr + 0.3) THEN 1
            WHEN cr.creat >= (cr.creat_low_past_7day * 1.5) THEN 1
            ELSE 0
          END AS aki_stage_creat
    FROM kdigo_creatinine cr
)

, uo_stg AS (
    SELECT
        uo.patientunitstayid
        , uo.chartoffset
        , uo.weight
        , uo.uo_rt_6hr
        , uo.uo_rt_12hr
        , uo.uo_rt_24hr
        , uo.uo_tm_6hr
        , uo.uo_tm_12hr
        , uo.uo_tm_24hr
        , CASE
            WHEN uo.uo_rt_6hr IS NULL THEN NULL
            WHEN uo.chartoffset <= 360 THEN 0
            WHEN uo.uo_tm_24hr >= 24 AND uo.uo_rt_24hr < 0.3 THEN 3
            WHEN uo.uo_tm_12hr >= 12 AND uo.uo_rt_12hr = 0 THEN 3
            WHEN uo.uo_tm_12hr >= 12 AND uo.uo_rt_12hr < 0.5 THEN 2
            WHEN uo.uo_tm_6hr >= 6 AND uo.uo_rt_6hr < 0.5 THEN 1
            ELSE 0
          END AS aki_stage_uo
    FROM kdigo_uo uo
)

, dialysis_events AS (
    SELECT
        d.patientunitstayid
        , d.chartoffset
        , d.dialysis_type
    FROM dialysis d
    WHERE d.chartoffset IS NOT NULL
)

, chronic_dialysis_flags AS (
    SELECT DISTINCT
        de.patientunitstayid
    FROM dialysis_events de
    WHERE de.dialysis_type IN (
        'chronic_SCUF'
        , 'chronic_intermittent_hemodialysis'
        , 'chronic_peritoneal_dialysis'
        , 'past_hemodialysis'
        , 'past peritoneal dialysis'
        , 'dialysis_graft'
        , 'av_fistula'
        , 'av_shunt'
    )
)

, aki_rrt_events AS (
    SELECT
        de.patientunitstayid
        , de.chartoffset
        , de.dialysis_type
    FROM dialysis_events de
    LEFT JOIN chronic_dialysis_flags cf
        ON de.patientunitstayid = cf.patientunitstayid
    WHERE de.dialysis_type IN (
            'acute_SCUF'
            , 'SLED'
            , 'CVVH'
            , 'CAVHD'
            , 'CVVHD'
            , 'acute_intermittent_hemodialysis'
            , 'acute_peritoneal_dialysis'
        )
        OR (
            de.dialysis_type IN (
                'unknown_SCUF'
                , 'unknown_intermittent_hemodialysis'
                , 'unknown_peritoneal_dialysis'
            )
            AND cf.patientunitstayid IS NULL
        )
)

, dialysis_output_events AS (
    SELECT
        de.patientunitstayid
        , de.chartoffset
    FROM dialysis_events de
    LEFT JOIN chronic_dialysis_flags cf
        ON de.patientunitstayid = cf.patientunitstayid
    WHERE de.dialysis_type = 'dialysis_output'
        AND (
            cf.patientunitstayid IS NULL
            OR EXISTS (
                SELECT 1
                FROM aki_rrt_events ar
                WHERE ar.patientunitstayid = de.patientunitstayid
                    AND ABS(ar.chartoffset - de.chartoffset) <= 720
            )
        )
)

, dialysis_stg AS (
    SELECT
        patientunitstayid
        , chartoffset
        , 3 AS aki_stage_rrt
    FROM (
        SELECT patientunitstayid, chartoffset FROM aki_rrt_events
        UNION ALL
        SELECT patientunitstayid, chartoffset FROM dialysis_output_events
    )
)

, tm_stg AS (
    SELECT patientunitstayid, chartoffset FROM cr_stg WHERE chartoffset IS NOT NULL
    UNION DISTINCT
    SELECT patientunitstayid, chartoffset FROM uo_stg WHERE chartoffset IS NOT NULL
    UNION DISTINCT
    SELECT patientunitstayid, chartoffset FROM dialysis_stg WHERE chartoffset IS NOT NULL
)

SELECT
    p.uniquepid
    , p.patienthealthsystemstayid
    , p.patientunitstayid
    , tm.chartoffset
    , cr.creat_low_past_7day
    , cr.creat_low_past_48hr
    , cr.creat
    , cr.aki_stage_creat
    , uo.uo_rt_6hr
    , uo.uo_rt_12hr
    , uo.uo_rt_24hr
    , uo.aki_stage_uo
    , dialysis.aki_stage_rrt AS aki_stage_dialysis
    , GREATEST(
        COALESCE(cr.aki_stage_creat, 0)
        , COALESCE(uo.aki_stage_uo, 0)
        , COALESCE(dialysis.aki_stage_rrt, 0)
      ) AS aki_stage
    , MAX(
        GREATEST(
            COALESCE(cr.aki_stage_creat, 0)
            , COALESCE(uo.aki_stage_uo, 0)
            , COALESCE(dialysis.aki_stage_rrt, 0)
        )
      ) OVER (
        PARTITION BY p.patientunitstayid
        ORDER BY tm.chartoffset
        RANGE BETWEEN 360 PRECEDING AND CURRENT ROW
      ) AS aki_stage_smoothed
FROM patient p
LEFT JOIN tm_stg tm
    ON p.patientunitstayid = tm.patientunitstayid
LEFT JOIN cr_stg cr
    ON p.patientunitstayid = cr.patientunitstayid
    AND tm.chartoffset = cr.chartoffset
LEFT JOIN uo_stg uo
    ON p.patientunitstayid = uo.patientunitstayid
    AND tm.chartoffset = uo.chartoffset
LEFT JOIN dialysis_stg dialysis
    ON p.patientunitstayid = dialysis.patientunitstayid
    AND tm.chartoffset = dialysis.chartoffset
WHERE tm.chartoffset IS NOT NULL
ORDER BY p.patientunitstayid, tm.chartoffset;

