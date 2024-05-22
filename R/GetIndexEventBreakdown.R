#' @export
getIndexEventBreakdown <- function(cohortIds,
                                   cdmDatabaseSchema,
                                   cohortDatabaseSchema,
                                   tempEmulationationSchema = getOption("sqlRenderTempEmulationSchema")) {
  sql <- "DROP TABLE #visit_occurrence_count;
          DROP TABLE #procedure_occurrence_count;
          DROP TABLE #condition_occurrence_count;
          DROP TABLE #measurement_count;
          DROP TABLE #visit_occurrence_count;

          SELECT cohort_definition_id,
          	visit_concept_id concept_id,
          	visit_source_concept_id source_concept_id,
          	COUNT(DISTINCT person_id) persons,
          	COUNT(*) records
          INTO #visit_occurrence_count
          FROM @cdm_database_schema.visit_occurrence dt
          INNER JOIN (
          	SELECT cohort_definition_id,
          		subject_id,
          		min(cohort_start_date) cohort_start_date
          	FROM @cohort_database_schema.@cohort_table
          	GROUP BY cohort_definition_id,
          		subject_id
          	) ct
          	ON dt.person_id = ct.subject_id
          		AND dt.visit_start_date = ct.cohort_start_date
          WHERE visit_concept_id IN (
          		SELECT DISTINCT concept_id
          		FROM #index_concepts
          		)
          	OR visit_source_concept_id IN (
          		SELECT DISTINCT concept_id
          		FROM #index_concepts
          		);

          SELECT cohort_definition_id,
          	procedure_concept_id concept_id,
          	procedure_source_concept_id source_concept_id,
          	COUNT(DISTINCT person_id) persons,
          	COUNT(*) records
          INTO #procedure_occurrence_count
          FROM @cdm_database_schema.procedure_occurrence dt
          INNER JOIN (
          	SELECT cohort_definition_id,
          		subject_id,
          		min(cohort_start_date) cohort_start_date
          	FROM @cohort_database_schema.@cohort_table
          	GROUP BY cohort_definition_id,
          		subject_id
          	) ct
          	ON dt.person_id = ct.subject_id
          		AND dt.procedure_start_date = ct.cohort_start_date
          WHERE procedure_concept_id IN (
          		SELECT DISTINCT concept_id
          		FROM #index_concepts
          		)
          	OR procedure_source_concept_id IN (
          		SELECT DISTINCT concept_id
          		FROM #index_concepts
          		);

          SELECT cohort_definition_id,
          	observation_concept_id concept_id,
          	observation_source_concept_id source_concept_id,
          	COUNT(DISTINCT person_id) persons,
          	COUNT(*) records
          INTO #observation_count
          FROM @cdm_database_schema.observation dt
          INNER JOIN (
          	SELECT cohort_definition_id,
          		subject_id,
          		min(cohort_start_date) cohort_start_date
          	FROM @cohort_database_schema.@cohort_table
          	GROUP BY cohort_definition_id,
          		subject_id
          	) ct
          	ON dt.person_id = ct.subject_id
          		AND dt.observation_start_date = ct.cohort_start_date
          WHERE observation_concept_id IN (
          		SELECT DISTINCT concept_id
          		FROM #index_concepts
          		)
          	OR observation_source_concept_id IN (
          		SELECT DISTINCT concept_id
          		FROM #index_concepts
          		);

          SELECT cohort_definition_id,
          	drug_concept_id concept_id,
          	drug_exposure_source_concept_id source_concept_id,
          	COUNT(DISTINCT person_id) persons,
          	COUNT(*) records
          INTO #observation_count
          FROM @cdm_database_schema.observation dt
          INNER JOIN (
          	SELECT cohort_definition_id,
          		subject_id,
          		min(cohort_start_date) cohort_start_date
          	FROM @cohort_database_schema.@cohort_table
          	GROUP BY cohort_definition_id,
          		subject_id
          	) ct
          	ON dt.person_id = ct.subject_id
          		AND dt.drug_start_date = ct.cohort_start_date
          WHERE drug_concept_id IN (
          		SELECT DISTINCT concept_id
          		FROM #index_concepts
          		)
          	OR drug_source_concept_id IN (
          		SELECT DISTINCT concept_id
          		FROM #index_concepts
          		);"
  
}