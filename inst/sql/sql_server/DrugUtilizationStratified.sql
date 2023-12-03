DROP TABLE IF EXISTS #numerator_counts;
DROP TABLE IF EXISTS #denominator_counts;

SELECT d.strata_id,
	COUNT(DISTINCT de.drug_exposure_id) numerator_record_count,
--	COUNT(DISTINCT CONCAT(CAST(de.drug_exposure_start_date AS VARCHAR(20)),
--	                      CAST(de.person_id AS VARCHAR(50)))) numerator_date_count,
	COUNT(DISTINCT de.person_id) numerator_person_count
INTO #numerator_counts
FROM @cdm_database_schema.drug_exposure de
INNER JOIN @cdm_database_schema.person p ON de.person_id = p.person_id
INNER JOIN @strata_table d ON YEAR(de.drug_exposure_start_date) - p.year_of_birth >= d.age_greater_than_or_equal_to
	AND YEAR(de.drug_exposure_start_date) - p.year_of_birth < d.age_lower_than
	AND p.gender_concept_id = d.gender_concept_id
	AND de.drug_exposure_start_date >= d.date_on_or_after
	AND de.drug_exposure_start_date < d.date_before
INNER JOIN @concept_id_table c ON de.drug_concept_id = c.concept_id
INNER JOIN @cdm_database_schema.observation_period op 
  ON p.person_id = op.person_id
  AND de.person_id = op.person_id
  AND de.drug_exposure_start_date >= op.observation_period_start_date
  AND de.drug_exposure_start_date <= op.observation_period_end_date
GROUP BY strata_id;

-- SELECT strata_id,
-- 	COUNT(DISTINCT person_id) denominator_person_count,
-- 	SUM(DATEDIFF(dd, start_date, end_date)) denominator_person_days
-- INTO #denominator_counts
-- FROM (
-- 	SELECT DISTINCT d.strata_id,
-- 		op.person_id,
-- 		CASE 
-- 			WHEN d.date_on_or_after > op.observation_period_start_date
-- 				THEN d.date_on_or_after
-- 			ELSE op.observation_period_start_date
-- 			END start_date,
-- 		CASE 
-- 			WHEN d.date_before > op.observation_period_end_date
-- 				THEN op.observation_period_end_date
-- 			ELSE d.date_before
-- 			END end_date
-- 	FROM @cdm_database_schema.observation_period op
-- 	INNER JOIN @cdm_database_schema.person p ON op.person_id = p.person_id
-- 	INNER JOIN @strata_table d ON p.gender_concept_id = d.gender_concept_id
-- 		AND (
-- 			observation_period_end_date >= d.date_on_or_after
-- 			AND observation_period_start_date <= d.date_before
-- 			)
-- 		AND
-- 		  YEAR(
--       		  CASE 
--       			WHEN d.date_on_or_after > op.observation_period_start_date
--       				THEN d.date_on_or_after
--       			ELSE op.observation_period_start_date
--       			END
--       		) - p.year_of_birth >= d.age_greater_than_or_equal_to
--     AND 
--       YEAR(
--       		  CASE 
--       			WHEN d.date_on_or_after > op.observation_period_start_date
--       				THEN d.date_on_or_after
--       			ELSE op.observation_period_start_date
--       			END
--       		) - p.year_of_birth <= d.age_lower_than
-- 	) F
-- GROUP BY strata_id;