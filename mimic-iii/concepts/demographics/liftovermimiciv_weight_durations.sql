--PJ: no neonates data, no echo data. no wt_stg0. some item ids from 3 arent in 4. in 3: admit, daily, birth are all weight_types.
-- This query extracts weights for adult ICU patients with start/stop times
-- if an admission weight is given, then this is assigned from intime to outtime
WITH wt_stg AS (
    SELECT
        c.icustay_id
        , c.charttime
        --, CASE WHEN c.itemid = 226512 THEN 'admit'
        , CASE WHEN c.itemid in (762,226512) THEN 'admit'
            ELSE 'daily' END AS weight_type
        -- TO DO: eliminate obvious outliers if there is a reasonable weight
        , c.valuenum AS weight
    FROM `physionet-data.mimiciii_clinical.chartevents` c
    WHERE c.valuenum IS NOT NULL
        AND c.itemid IN
        (
            762,226512 -- Admit Wt
            , 763,224639 -- Daily Weight
        )
        AND c.valuenum > 0
)

-- assign ascending row number
, wt_stg1 AS (
    SELECT
        icustay_id
        , charttime
        , weight_type
        , weight
        , ROW_NUMBER() OVER (
            PARTITION BY icustay_id, weight_type ORDER BY charttime
        ) AS rn
    FROM wt_stg
    WHERE weight IS NOT NULL
)

-- change charttime to intime for the first admission weight recorded
, wt_stg2 AS (
    SELECT
        wt_stg1.icustay_id
        , ie.intime, ie.outtime
        , wt_stg1.weight_type
        , CASE WHEN wt_stg1.weight_type = 'admit' AND wt_stg1.rn = 1
            THEN DATETIME_SUB(ie.intime, INTERVAL '2' HOUR)
            ELSE wt_stg1.charttime END AS starttime
        , wt_stg1.weight
    FROM wt_stg1
    INNER JOIN `physionet-data.mimiciii_clinical.icustays` ie
        ON ie.icustay_id = wt_stg1.icustay_id
)

, wt_stg3 AS (
    SELECT
        icustay_id
        , intime, outtime
        , starttime
        , COALESCE(
            LEAD(starttime) OVER (PARTITION BY icustay_id ORDER BY starttime)
            , DATETIME_ADD(outtime, INTERVAL '2' HOUR)
        ) AS endtime
        , weight
        , weight_type
    FROM wt_stg2
)

-- this table is the start/stop times from admit/daily weight in charted data
, wt1 AS (
    SELECT
        icustay_id
        , starttime
        , COALESCE(
            endtime
            , LEAD(
                starttime
            ) OVER (PARTITION BY icustay_id ORDER BY starttime)
            -- impute ICU discharge as the end of the final weight measurement
            -- plus a 2 hour "fuzziness" window
            , DATETIME_ADD(outtime, INTERVAL '2' HOUR)
        ) AS endtime
        , weight
        , weight_type
    FROM wt_stg3
)

-- if the intime for the patient is < the first charted daily weight
-- then we will have a "gap" at the start of their stay
-- to prevent this, we look for these gaps and backfill the first weight
-- this adds (153255-149657)=3598 rows, meaning this fix helps for up
-- to 3598 stay_id
, wt_fix AS (
    SELECT ie.icustay_id
        -- we add a 2 hour "fuzziness" window
        , DATETIME_SUB(ie.intime, INTERVAL '2' HOUR) AS starttime
        , wt.starttime AS endtime
        , wt.weight
        , wt.weight_type
    FROM `physionet-data.mimiciii_clinical.icustays` ie
    INNER JOIN
        -- the below subquery returns one row for each unique icustay_id
        -- the row contains: the first starttime and the corresponding weight
        (
            SELECT wt1.icustay_id, wt1.starttime, wt1.weight
                , weight_type
                , ROW_NUMBER() OVER (
                    PARTITION BY wt1.icustay_id ORDER BY wt1.starttime
                ) AS rn
            FROM wt1
        ) wt
        ON ie.icustay_id = wt.icustay_id
            AND wt.rn = 1
            AND ie.intime < wt.starttime
)

-- add the backfill rows to the main weight table
SELECT
    wt1.icustay_id
    , wt1.starttime
    , wt1.endtime
    , wt1.weight
    , wt1.weight_type
FROM wt1
UNION ALL
SELECT
    wt_fix.icustay_id
    , wt_fix.starttime
    , wt_fix.endtime
    , wt_fix.weight
    , wt_fix.weight_type
FROM wt_fix;
