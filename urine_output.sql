-- Urine output events for eICU patients aggregated by chart offset.
WITH labeled_io AS (
    SELECT
        io.patientunitstayid
        , io.intakeoutputoffset AS chartoffset
        , io.cellvaluenumeric
        , CASE
            WHEN io.cellpath NOT LIKE 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|%' THEN 0
            WHEN io.cellpath IN (
                'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|3 way foley'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|3 Way Foley'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Actual Urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Adjusted total UO NOC end shift'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|BRP (urine)'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|BRP (Urine)'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|condome cath urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|diaper urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|inc of urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|incontient urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Incontient urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Incontient Urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|incontinence of urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Incontinence-urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|incontinence/ voids urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|incontinent of urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|INCONTINENT OF URINE'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Incontinent UOP'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|incontinent urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Incontinent (urine)'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Incontinent Urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|incontinent urine counts'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|incont of urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|incont. of urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|incont. of urine count'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|incont. of urine count'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|incont urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|incont. urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Incont. urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Incont. Urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|inc urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|inc. urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Inc. urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Inc Urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|indwelling foley'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Indwelling Foley'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight Catheter-Foley'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight Catheterization Urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight Cath UOP'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|straight cath urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight Cath Urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|strait cath Urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Suprapubic Urine Output'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|true urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|True Urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|True Urine out'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|unmeasured urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Unmeasured Urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|unmeasured urine output'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urethal Catheter'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urethral Catheter'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|urinary output 7AM - 7 PM'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|urinary output 7AM-7PM'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|URINE'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|URINE'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|URINE CATHETER'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Intermittent/Straight Cath (mL)'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|straightcath'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|straight cath'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight cath'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight  cath'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight Cath'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight  Cath'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Straight Cath'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-LEFT PCN TUBE'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-L Nephrostomy'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-L Nephrostomy Tube'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-Nephrostomy'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-right nephrostomy'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-RIGHT Nephrouretero Stent Urine Output'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-R nephrostomy'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-R Nephrostomy'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-R. Nephrostomy'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-R Nephrostomy Tube'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-Rt Nephrectomy'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-stent'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-straight cath'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-suprapubic'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-Texas Cath'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-Urine'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output-Urine Output'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine, R neph:'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine-straight cath'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Straight Cath'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|urine (void)'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine- void'
                , 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine, void:'
            ) THEN 1
            WHEN io.cellpath LIKE 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|foley%'
                AND LOWER(io.cellpath) NOT LIKE '%pacu%'
                AND LOWER(io.cellpath) NOT LIKE '%or%'
                AND LOWER(io.cellpath) NOT LIKE '%ir%'
            THEN 1
            WHEN io.cellpath LIKE 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Output%Urinary Catheter%'
            THEN 1
            WHEN io.cellpath LIKE 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Output%Urethral Catheter%'
            THEN 1
            WHEN io.cellpath LIKE 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urine Output (mL)%'
            THEN 1
            WHEN io.cellpath LIKE 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Output%External Urethral%'
            THEN 1
            WHEN io.cellpath LIKE 'flowsheet|Flowsheet Cell Labels|I&O|Output (ml)|Urinary Catheter Output%'
            THEN 1
            ELSE 0
        END AS cellpath_is_urine
    FROM intakeoutput io
    WHERE io.cellvaluenumeric IS NOT NULL
)
SELECT
    patientunitstayid
    , chartoffset
    , SUM(cellvaluenumeric) AS urineoutput
FROM labeled_io
WHERE cellpath_is_urine = 1
GROUP BY patientunitstayid, chartoffset
ORDER BY patientunitstayid, chartoffset;
