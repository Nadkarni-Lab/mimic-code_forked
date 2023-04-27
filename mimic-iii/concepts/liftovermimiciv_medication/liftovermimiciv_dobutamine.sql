-- This query extracts dose+durations of dobutamine administration
-- Local hospital dosage guidance: 2 mcg/kg/min (low) - 40 mcg/kg/min (max)
SELECT
    icustay_id, linkorderid
    -- all rows in mcg/kg/min
    , rate AS vaso_rate
    , amount AS vaso_amount
    , starttime
    , endtime
FROM `physionet-data.mimiciii_clinical.inputevents`
WHERE itemid = 221653 -- dobutamine
