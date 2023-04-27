-- create a table which has fuzzy boundaries on hospital admission
-- involves first creating a lag/lead version of disch/admit time
-- get first/last heart rate measurement during hospitalization for each icustay_id
WITH t1 AS (
    SELECT ce.icustay_id
        , MIN(charttime) AS intime_hr
        , MAX(charttime) AS outtime_hr
    FROM `physionet-data.mimiciii_clinical.chartevents` ce
    -- only look at heart rate
    WHERE ce.itemid = 220045
    GROUP BY ce.icustay_id
)

-- add in subject_id/hadm_id
SELECT
    ie.subject_id, ie.hadm_id, ie.icustay_id
    , t1.intime_hr
    , t1.outtime_hr
FROM `physionet-data.mimiciii_clinical.icustays` ie
LEFT JOIN t1
          ON ie.icustay_id = t1.icustay_id;
