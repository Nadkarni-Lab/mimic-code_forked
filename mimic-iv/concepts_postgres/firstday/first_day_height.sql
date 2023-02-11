-- THIS SCRIPT IS AUTOMATICALLY GENERATED. DO NOT EDIT IT DIRECTLY.
DROP TABLE IF EXISTS first_day_height; CREATE TABLE first_day_height AS
-- This query extracts heights for adult ICU patients.
-- It uses all information from the patient's first ICU day.
-- This is done for consistency with other queries.
-- Height is unlikely to change throughout a patient's stay.

SELECT
    ie.subject_id
    , ie.stay_id
    , ROUND(CAST(AVG(height) AS NUMERIC), 2) AS height
FROM mimiciv_icu.icustays ie
LEFT JOIN mimiciv_derived.height ht
    ON ie.stay_id = ht.stay_id
        AND ht.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
        AND ht.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
GROUP BY ie.subject_id, ie.stay_id;
