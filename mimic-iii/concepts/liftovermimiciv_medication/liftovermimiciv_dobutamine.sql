-- This query extracts dose+durations of dobutamine administration
-- Local hospital dosage guidance: 2 mcg/kg/min (low) - 40 mcg/kg/min (max)
/*SELECT
    icustay_id, linkorderid
    -- all rows in mcg/kg/min
    , rate AS vaso_rate
    , amount AS vaso_amount
    , starttime
    , endtime
FROM `physionet-data.mimiciii_clinical.inputevents`
WHERE itemid = 221653 -- dobutamine*/

SELECT
    COALESCE(iemv.icustay_id, iecv.icustay_id, 0) AS icustay_id
        , COALESCE(iemv.linkorderid, iecv.linkorderid, 0) AS linkorderid
    -- all rows in mcg/kg/min
    , COALESCE(iemv.rate, iecv.rate, 0) AS vaso_rate
    , COALESCE(iemv.amount, iecv.amount, 0) AS vaso_amount
    , iemv.starttime AS starttime
    , iemv.endtime AS endtime
    , iecv.storetime AS storetime
    , iecv.charttime AS charttime
FROM `physionet-data.mimiciii_clinical.inputevents_mv` iemv
LEFT JOIN `physionet-data.mimiciii_clinical.inputevents_cv` iecv
    ON iemv.icustay_id = iecv.icustay_id
and iemv.itemid = iecv.itemid
WHERE iemv.itemid = 221653 -- dobutamine
