-- ------------------------------------------------------------------
-- Title: Sequential Organ Failure Assessment (SOFA)
-- This query extracts the sequential organ failure assessment
-- (formally: sepsis-related organ failure assessment).
-- This score is a measure of organ failure for patients in the ICU.
-- The score is calculated for **every hour** of the patient's ICU stay.
-- However, as the calculation window is 24 hours, care should be
-- taken when using the score before the end of the first day,
-- as the data window is limited.
-- ------------------------------------------------------------------

-- Reference for SOFA:
--    Jean-Louis Vincent, Rui Moreno, Jukka Takala, Sheila Willatts,
--    Arnaldo De Mendonça, Hajo Bruining, C. K. Reinhart,
--    Peter M Suter, and L. G. Thijs.
--    "The SOFA (Sepsis-related Organ Failure Assessment) score to
--     describe organ dysfunction/failure."
--    Intensive care medicine 22, no. 7 (1996): 707-710.

-- Variables used in SOFA:
--  GCS, MAP, FiO2, Ventilation status (chartevents)
--  Creatinine, Bilirubin, FiO2, PaO2, Platelets (labevents)
--  Dopamine, Dobutamine, Epinephrine, Norepinephrine (inputevents)
--  Urine output (outputevents)

-- use icustay_hourly to get a row for every hour the patient was in the ICU
-- all of our joins to data will use these times
-- to extract data pertinent to only that hour
WITH co AS (
    SELECT ih.icustay_id, ie.hadm_id
        , hr
        -- start/endtime can be used to filter to values within this hour
        , DATETIME_SUB(ih.endtime, INTERVAL '1' HOUR) AS starttime
        , ih.endtime
    FROM `physionet-data.mimiciii_clinical.liftovermimiciv_icustay_hourly` ih
    INNER JOIN `physionet-data.mimiciii_clinical.icustays` ie
        ON ih.icustay_id = ie.icustay_id
)

, pafi AS (
    -- join blood gas to ventilation durations to determine if patient was vent
    SELECT ie.icustay_id
        , bg.charttime
        -- because pafi has an interaction between vent/PaO2:FiO2,
        -- we need two columns for the score
        -- it can happen that the lowest unventilated PaO2/FiO2 is 68,
        -- but the lowest ventilated PaO2/FiO2 is 120
        -- in this case, the SOFA score is 3, *not* 4.
        , CASE
            WHEN vd.icustay_id IS NULL THEN pao2fio2ratio ELSE null
        END AS pao2fio2ratio_novent
        , CASE
            WHEN vd.icustay_id IS NOT NULL THEN pao2fio2ratio ELSE null
        END AS pao2fio2ratio_vent
    FROM `physionet-data.mimiciii_clinical.icustays` ie
    INNER JOIN `physionet-data.mimiciii_clinical.liftovermimiciv_bg` bg
        ON ie.subject_id = bg.subject_id
    LEFT JOIN `physionet-data.mimiciii_clinical.liftovermimiciv_ventilation` vd
        ON ie.icustay_id = vd.icustay_id
            AND bg.charttime >= vd.starttime
            AND bg.charttime <= vd.endtime
            AND vd.ventilation_status = 'InvasiveVent'
    WHERE specimen = 'ART.'
)

, vs AS (

    SELECT co.icustay_id, co.hr
        -- vitals
        , MIN(vs.mbp) AS meanbp_min
    FROM co
    LEFT JOIN `physionet-data.mimiciii_clinical.liftovermimiciv_vitalsign` vs
        ON co.icustay_id = vs.icustay_id
            AND co.starttime < vs.charttime
            AND co.endtime >= vs.charttime
    GROUP BY co.icustay_id, co.hr
)

, gcs AS (
    SELECT co.icustay_id, co.hr
        -- gcs
        , MIN(gcs.gcs) AS gcs_min
    FROM co
    LEFT JOIN `physionet-data.mimiciii_clinical.liftovermimiciv_gcs` gcs
        ON co.icustay_id = gcs.icustay_id
            AND co.starttime < gcs.charttime
            AND co.endtime >= gcs.charttime
    GROUP BY co.icustay_id, co.hr
)

, bili AS (
    SELECT co.icustay_id, co.hr
        , MAX(enz.bilirubin_total) AS bilirubin_max
    FROM co
    LEFT JOIN `physionet-data.mimiciii_clinical.liftovermimiciv_enzyme` enz
        ON co.hadm_id = enz.hadm_id
            AND co.starttime < enz.charttime
            AND co.endtime >= enz.charttime
    GROUP BY co.icustay_id, co.hr
)

, cr AS (
    SELECT co.icustay_id, co.hr
        , MAX(chem.creatinine) AS creatinine_max
    FROM co
    LEFT JOIN `physionet-data.mimiciii_clinical.liftoermimiciv_chemistry` chem
        ON co.hadm_id = chem.hadm_id
            AND co.starttime < chem.charttime
            AND co.endtime >= chem.charttime
    GROUP BY co.icustay_id, co.hr
)

, plt AS (
    SELECT co.icustay_id, co.hr
        , MIN(cbc.platelet) AS platelet_min
    FROM co
    LEFT JOIN `physionet-data.mimiciii_clinical.liftovermimiciv_complete_blood_count` cbc
        ON co.hadm_id = cbc.hadm_id
            AND co.starttime < cbc.charttime
            AND co.endtime >= cbc.charttime
    GROUP BY co.icustay_id, co.hr
)

, pf AS (
    SELECT co.icustay_id, co.hr
        , MIN(pafi.pao2fio2ratio_novent) AS pao2fio2ratio_novent
        , MIN(pafi.pao2fio2ratio_vent) AS pao2fio2ratio_vent
    FROM co
    -- bring in blood gases that occurred during this hour
    LEFT JOIN pafi
        ON co.icustay_id = pafi.icustay_id
            AND co.starttime < pafi.charttime
            AND co.endtime >= pafi.charttime
    GROUP BY co.icustay_id, co.hr
)

-- sum uo separately to prevent duplicating values
, uo AS (
    SELECT co.icustay_id, co.hr
        -- uo
        , MAX(
            CASE WHEN uo.uo_tm_24hr >= 22 AND uo.uo_tm_24hr <= 30
                THEN uo.urineoutput_24hr / uo.uo_tm_24hr * 24
            END) AS uo_24hr
    FROM co
    LEFT JOIN `physionet-data.mimiciii_clinical.liftovermimiciv_urine_output_rate` uo
        ON co.icustay_id = uo.icustay_id
            AND co.starttime < uo.charttime
            AND co.endtime >= uo.charttime
    GROUP BY co.icustay_id, co.hr
)

-- collapse vasopressors into 1 row per hour
-- also ensures only 1 row per chart time
, vaso AS (
    SELECT
        co.icustay_id
        , co.hr
        , MAX(epi.vaso_rate) AS rate_epinephrine
        , MAX(nor.vaso_rate) AS rate_norepinephrine
        , MAX(dop.vaso_rate) AS rate_dopamine
        , MAX(dob.vaso_rate) AS rate_dobutamine
    FROM co
    LEFT JOIN `physionet-data.mimiciii_clinical.liftovermimiciv_epinephrine` epi
        ON co.icustay_id = epi.icustay_id
            AND co.endtime > epi.starttime
            AND co.endtime <= epi.endtime
    LEFT JOIN `physionet-data.mimiciii_clinical.liftovermimiciv_norepinephrine` nor
        ON co.icustay_id = nor.icustay_id
            AND co.endtime > nor.starttime
            AND co.endtime <= nor.endtime
    LEFT JOIN `physionet-data.mimiciii_clinical.liftovermimiciv_dopamine` dop
        ON co.icustay_id = dop.icustay_id
            AND co.endtime > dop.starttime
            AND co.endtime <= dop.endtime
    LEFT JOIN `physionet-data.mimiciii_clinical.liftovermimiciv_dobutamine` dob
        ON co.icustay_id = dob.icustay_id
            AND co.endtime > dob.starttime
            AND co.endtime <= dob.endtime
    WHERE epi.icustay_id IS NOT NULL
        OR nor.icustay_id IS NOT NULL
        OR dop.icustay_id IS NOT NULL
        OR dob.icustay_id IS NOT NULL
    GROUP BY co.icustay_id, co.hr
)

, scorecomp AS (
    SELECT
        co.icustay_id
        , co.hr
        , co.starttime, co.endtime
        , pf.pao2fio2ratio_novent
        , pf.pao2fio2ratio_vent
        , vaso.rate_epinephrine
        , vaso.rate_norepinephrine
        , vaso.rate_dopamine
        , vaso.rate_dobutamine
        , vs.meanbp_min
        , gcs.gcs_min
        -- uo
        , uo.uo_24hr
        -- labs
        , bili.bilirubin_max
        , cr.creatinine_max
        , plt.platelet_min
    FROM co
    LEFT JOIN vs
        ON co.icustay_id = vs.icustay_id
            AND co.hr = vs.hr
    LEFT JOIN gcs
        ON co.icustay_id = gcs.icustay_id
            AND co.hr = gcs.hr
    LEFT JOIN bili
        ON co.icustay_id = bili.icustay_id
            AND co.hr = bili.hr
    LEFT JOIN cr
        ON co.icustay_id = cr.icustay_id
            AND co.hr = cr.hr
    LEFT JOIN plt
        ON co.icustay_id = plt.icustay_id
            AND co.hr = plt.hr
    LEFT JOIN pf
        ON co.icustay_id = pf.icustay_id
            AND co.hr = pf.hr
    LEFT JOIN uo
        ON co.icustay_id = uo.icustay_id
            AND co.hr = uo.hr
    LEFT JOIN vaso
        ON co.icustay_id = vaso.icustay_id
            AND co.hr = vaso.hr
)

, scorecalc AS (
    -- Calculate the final score
    -- note that if the underlying data is missing,
    -- the component is null
    -- eventually these are treated as 0 (normal),
    -- but knowing when data is missing is useful for debugging
    SELECT scorecomp.*
        -- Respiration
        , CASE
            WHEN pao2fio2ratio_vent < 100 THEN 4
            WHEN pao2fio2ratio_vent < 200 THEN 3
            WHEN pao2fio2ratio_novent < 300 THEN 2
            WHEN pao2fio2ratio_vent < 300 THEN 2
            WHEN pao2fio2ratio_novent < 400 THEN 1
            WHEN pao2fio2ratio_vent < 400 THEN 1
            WHEN
                COALESCE(
                    pao2fio2ratio_vent, pao2fio2ratio_novent
                ) IS NULL THEN null
            ELSE 0
        END AS respiration

        -- Coagulation
        , CASE
            WHEN platelet_min < 20 THEN 4
            WHEN platelet_min < 50 THEN 3
            WHEN platelet_min < 100 THEN 2
            WHEN platelet_min < 150 THEN 1
            WHEN platelet_min IS NULL THEN null
            ELSE 0
        END AS coagulation

        -- Liver
        , CASE
            -- Bilirubin checks in mg/dL
            WHEN bilirubin_max >= 12.0 THEN 4
            WHEN bilirubin_max >= 6.0 THEN 3
            WHEN bilirubin_max >= 2.0 THEN 2
            WHEN bilirubin_max >= 1.2 THEN 1
            WHEN bilirubin_max IS NULL THEN null
            ELSE 0
        END AS liver

        -- Cardiovascular
        , CASE
            WHEN rate_dopamine > 15
                OR rate_epinephrine > 0.1
                OR rate_norepinephrine > 0.1
                THEN 4
            WHEN rate_dopamine > 5
                OR rate_epinephrine <= 0.1
                OR rate_norepinephrine <= 0.1
                THEN 3
            WHEN rate_dopamine > 0
                OR rate_dobutamine > 0
                THEN 2
            WHEN meanbp_min < 70 THEN 1
            WHEN
                COALESCE(
                    meanbp_min
                    , rate_dopamine
                    , rate_dobutamine
                    , rate_epinephrine
                    , rate_norepinephrine
                ) IS NULL THEN null
            ELSE 0
        END AS cardiovascular

        -- Neurological failure (GCS)
        , CASE
            WHEN (gcs_min >= 13 AND gcs_min <= 14) THEN 1
            WHEN (gcs_min >= 10 AND gcs_min <= 12) THEN 2
            WHEN (gcs_min >= 6 AND gcs_min <= 9) THEN 3
            WHEN gcs_min < 6 THEN 4
            WHEN gcs_min IS NULL THEN null
            ELSE 0
        END AS cns

        -- Renal failure - high creatinine or low urine output
        , CASE
            WHEN (creatinine_max >= 5.0) THEN 4
            WHEN uo_24hr < 200 THEN 4
            WHEN (creatinine_max >= 3.5 AND creatinine_max < 5.0) THEN 3
            WHEN uo_24hr < 500 THEN 3
            WHEN (creatinine_max >= 2.0 AND creatinine_max < 3.5) THEN 2
            WHEN (creatinine_max >= 1.2 AND creatinine_max < 2.0) THEN 1
            WHEN COALESCE(uo_24hr, creatinine_max) IS NULL THEN null
            ELSE 0
        END AS renal
    FROM scorecomp
)

, score_final AS (
    SELECT s.*
        -- Combine all the scores to get SOFA
        -- Impute 0 if the score is missing
        -- the window function takes the max over the last 24 hours
        , COALESCE(
            MAX(respiration) OVER w
            , 0) AS respiration_24hours
        , COALESCE(
            MAX(coagulation) OVER w
            , 0) AS coagulation_24hours
        , COALESCE(
            MAX(liver) OVER w
            , 0) AS liver_24hours
        , COALESCE(
            MAX(cardiovascular) OVER w
            , 0) AS cardiovascular_24hours
        , COALESCE(
            MAX(cns) OVER w
            , 0) AS cns_24hours
        , COALESCE(
            MAX(renal) OVER w
            , 0) AS renal_24hours

        -- sum together data for final SOFA
        , COALESCE(
            MAX(respiration) OVER w
            , 0)
        + COALESCE(
            MAX(coagulation) OVER w
            , 0)
        + COALESCE(
            MAX(liver) OVER w
            , 0)
        + COALESCE(
            MAX(cardiovascular) OVER w
            , 0)
        + COALESCE(
            MAX(cns) OVER w
            , 0)
        + COALESCE(
            MAX(renal) OVER w
            , 0)
        AS sofa_24hours
    FROM scorecalc s
    WINDOW w AS
        (
            PARTITION BY icustay_id
            ORDER BY hr
            ROWS BETWEEN 23 PRECEDING AND 0 FOLLOWING
        )
)

SELECT * FROM score_final
WHERE hr >= 0;
