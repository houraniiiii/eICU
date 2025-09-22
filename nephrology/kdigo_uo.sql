WITH base_uo AS (
    SELECT
        uo.patientunitstayid
        , uo.chartoffset
        , uo.urineoutput
    FROM urine_output uo
    INNER JOIN patient p
        ON p.patientunitstayid = uo.patientunitstayid
)

, uo_offsets AS (
    SELECT
        b.patientunitstayid
        , b.chartoffset
        , b.urineoutput
        , LAG(b.chartoffset) OVER (
            PARTITION BY b.patientunitstayid
            ORDER BY b.chartoffset
        ) AS prev_chartoffset
    FROM base_uo b
)

, uo_stg1 AS (
    SELECT
        u.patientunitstayid
        , u.chartoffset
        , u.urineoutput
        , CASE
            WHEN u.prev_chartoffset IS NULL THEN 1.0
            ELSE GREATEST((u.chartoffset - u.prev_chartoffset) / 60.0, 0.0)
        END AS hours_since_previous_row
        , CAST(u.chartoffset AS DOUBLE PRECISION) AS minutes_since_admit
    FROM uo_offsets u
)

, uo_stg2 AS (
    SELECT
        u.patientunitstayid
        , u.chartoffset
        , u.urineoutput
        , u.hours_since_previous_row
        , SUM(u.urineoutput) OVER (
            PARTITION BY u.patientunitstayid
            ORDER BY u.minutes_since_admit
            RANGE BETWEEN 360 PRECEDING AND CURRENT ROW
        ) AS urineoutput_6hr
        , SUM(u.urineoutput) OVER (
            PARTITION BY u.patientunitstayid
            ORDER BY u.minutes_since_admit
            RANGE BETWEEN 720 PRECEDING AND CURRENT ROW
        ) AS urineoutput_12hr
        , SUM(u.urineoutput) OVER (
            PARTITION BY u.patientunitstayid
            ORDER BY u.minutes_since_admit
            RANGE BETWEEN 1440 PRECEDING AND CURRENT ROW
        ) AS urineoutput_24hr
        , SUM(u.hours_since_previous_row) OVER (
            PARTITION BY u.patientunitstayid
            ORDER BY u.minutes_since_admit
            RANGE BETWEEN 360 PRECEDING AND CURRENT ROW
        ) AS uo_tm_6hr
        , SUM(u.hours_since_previous_row) OVER (
            PARTITION BY u.patientunitstayid
            ORDER BY u.minutes_since_admit
            RANGE BETWEEN 720 PRECEDING AND CURRENT ROW
        ) AS uo_tm_12hr
        , SUM(u.hours_since_previous_row) OVER (
            PARTITION BY u.patientunitstayid
            ORDER BY u.minutes_since_admit
            RANGE BETWEEN 1440 PRECEDING AND CURRENT ROW
        ) AS uo_tm_24hr
    FROM uo_stg1 u
)

SELECT
    ur.patientunitstayid
    , ur.chartoffset
    , wd.weight
    , ur.urineoutput_6hr
    , ur.urineoutput_12hr
    , ur.urineoutput_24hr
    , CASE
        WHEN wd.weight IS NOT NULL
            AND ur.uo_tm_6hr >= 6
            AND ur.uo_tm_6hr < 12
            AND ur.uo_tm_6hr > 0
        THEN ROUND((ur.urineoutput_6hr / wd.weight / ur.uo_tm_6hr)::numeric, 4)
        ELSE NULL
      END AS uo_rt_6hr
    , CASE
        WHEN wd.weight IS NOT NULL
            AND ur.uo_tm_12hr >= 12
            AND ur.uo_tm_12hr > 0
        THEN ROUND((ur.urineoutput_12hr / wd.weight / ur.uo_tm_12hr)::numeric, 4)
        ELSE NULL
      END AS uo_rt_12hr
    , CASE
        WHEN wd.weight IS NOT NULL
            AND ur.uo_tm_24hr >= 24
            AND ur.uo_tm_24hr > 0
        THEN ROUND((ur.urineoutput_24hr / wd.weight / ur.uo_tm_24hr)::numeric, 4)
        ELSE NULL
      END AS uo_rt_24hr
    , ur.uo_tm_6hr
    , ur.uo_tm_12hr
    , ur.uo_tm_24hr
FROM uo_stg2 ur
LEFT JOIN weight_durations wd
    ON wd.patientunitstayid = ur.patientunitstayid
    AND ur.chartoffset >= wd.startoffset
    AND (wd.endoffset IS NULL OR ur.chartoffset < wd.endoffset)
ORDER BY ur.patientunitstayid, ur.chartoffset;
