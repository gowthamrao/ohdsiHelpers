#' @export
getIndexEventBreakdown <- function(cohortIds,
                                   connectionDetails = NULL,
                                   connection = NULL,
                                   cdmDatabaseSchema,
                                   cohortDatabaseSchema,
                                   cohortTableName,
                                   tempEmulationationSchema = getOption("sqlRenderTempEmulationSchema"),
                                   indexConceptIds) {
  checkmate::assertDataFrame(x = indexConceptIds,
                             min.rows = 1,
                             ncols = 1)
  checkmate::assertCharacter(x = colnames(indexConceptIds), fixed = "conceptId")
  
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  DatabaseConnector::insertTable(
    connection = connection,
    databaseSchema = NULL,
    tableName = '#index_concepts',
    dropTableIfExists = TRUE,
    createTable = TRUE,
    data = indexConceptIds |> 
      dplyr::distinct(),
    tempTable = TRUE,
    tempEmulationSchema = tempEmulationationSchema,
    camelCaseToSnakeCase = TRUE
  )
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = " DROP TABLE IF EXISTS #cohort_index;
            DROP TABLE IF EXISTS #index_concepts_2;
            
            --HINT DISTRIBUTE_ON_KEY(concept_id)
            SELECT DISTINCT concept_id
            INTO #index_concepts_2
            FROM #index_concepts;

            --HINT SORT_ON_KEY(INTERLEAVED:cohort_start_date)
            --HINT DISTRIBUTE_ON_KEY(subject_id)
            SELECT subject_id,
          		min(cohort_start_date) cohort_start_date
          	INTO #cohort_index
          	FROM @cohort_database_schema.@cohort_table
          	WHERE cohort_definition_id IN (@cohort_ids)
          	GROUP BY subject_id;
            
            DROP TABLE IF EXISTS #index_concepts;",
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTableName,
    cohort_ids = cohortIds
  )
  
  browser()
  visitCount <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = " SELECT visit_concept_id concept_id,
            	COUNT(DISTINCT person_id) persons,
            	COUNT(*) records
            FROM @cdm_database_schema.visit_occurrence dt
            INNER JOIN #cohort_index ct
            ON dt.person_id = ct.subject_id
            	AND dt.visit_start_date = ct.cohort_start_date
            INNER JOIN #index_concepts_2 ic
            ON visit_concept_id = ic.concept_id
            WHERE visit_concept_id > 0
            GROUP BY visit_concept_id;",
    cdm_database_schema = cdmDatabaseSchema,
    snakeCaseToCamelCase = TRUE
  ) |>
    dplyr::tibble()
  
  visitSourceCount <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = " SELECT visit_source_concept_id concept_id,
            	COUNT(DISTINCT person_id) persons,
            	COUNT(*) records
            FROM @cdm_database_schema.visit_occurrence dt
            INNER JOIN #cohort_index ct
            ON dt.person_id = ct.subject_id
            	AND dt.visit_start_date = ct.cohort_start_date
            INNER JOIN #index_concepts_2 ic
            ON visit_source_concept_id = ic.concept_id
            WHERE visit_concept_id != visit_source_concept_id
              AND visit_source_concept_id > 0
            GROUP BY visit_source_concept_id;",
    cdm_database_schema = cdmDatabaseSchema,
    snakeCaseToCamelCase = TRUE
  ) |>
    dplyr::tibble()
  
  procedureCount <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = " SELECT procedure_concept_id concept_id,
            	COUNT(DISTINCT person_id) persons,
              COUNT(*) records
            FROM @cdm_database_schema.procedure_occurrence dt
            INNER JOIN #cohort_index ct
            ON dt.person_id = ct.subject_id
            	AND dt.procedure_date = ct.cohort_start_date
            INNER JOIN #index_concepts_2 ic
            ON procedure_concept_id = ic.concept_id
            WHERE procedure_concept_id > 0
            GROUP BY procedure_concept_id;",
    cdm_database_schema = cdmDatabaseSchema,
    snakeCaseToCamelCase = TRUE
  ) |>
    dplyr::tibble()
  
  procedureSourceCount <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = " SELECT procedure_source_concept_id concept_id,
              	COUNT(DISTINCT person_id) persons,
                COUNT(*) records
              FROM @cdm_database_schema.procedure_occurrence dt
              INNER JOIN #cohort_index ct
              ON dt.person_id = ct.subject_id
              	AND dt.procedure_date = ct.cohort_start_date
              INNER JOIN #index_concepts_2 ic
              ON procedure_source_concept_id = ic.concept_id
              WHERE procedure_source_concept_id != procedure_concept_id
                AND procedure_source_concept_id > 0
              GROUP BY procedure_source_concept_id;",
      cdm_database_schema = cdmDatabaseSchema,
      snakeCaseToCamelCase = TRUE
    ) |>
    dplyr::tibble()
  
  drugExposureCount <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT drug_concept_id concept_id,
            	COUNT(DISTINCT person_id) persons,
            	COUNT(*) records
            FROM @cdm_database_schema.drug_exposure dt
            INNER JOIN #cohort_index ct
           	ON dt.person_id = ct.subject_id
           		AND dt.drug_exposure_start_date = ct.cohort_start_date
            INNER JOIN #index_concepts_2 ic
            ON drug_concept_id = ic.concept_id
            WHERE drug_concept_id > 0
            GROUP BY drug_concept_id;",
    cdm_database_schema = cdmDatabaseSchema,
    snakeCaseToCamelCase = TRUE
  ) |>
    dplyr::tibble()
  
  drugExposureSourceCount <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT drug_source_concept_id source_concept_id,
              	COUNT(DISTINCT person_id) persons,
              	COUNT(*) records
              FROM @cdm_database_schema.drug_exposure dt
              INNER JOIN #cohort_index ct
             	ON dt.person_id = ct.subject_id
             		AND dt.drug_exposure_start_date = ct.cohort_start_date
              INNER JOIN #index_concepts_2 ic
              ON drug_source_concept_id = ic.concept_id
              WHERE drug_source_concept_id != drug_concept_id
                AND drug_source_concept_id > 0
              GROUP BY drug_source_concept_id;",
      cdm_database_schema = cdmDatabaseSchema,
      snakeCaseToCamelCase = TRUE
    ) |>
    dplyr::tibble()
  
  observationCount <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT observation_concept_id concept_id,
            	COUNT(DISTINCT person_id) persons,
            	COUNT(*) records
            FROM @cdm_database_schema.observation dt
            INNER JOIN #cohort_index ct
          	ON dt.person_id = ct.subject_id
          		AND dt.observation_date = ct.cohort_start_date
            INNER JOIN #index_concepts_2 ic
            ON observation_concept_id = ic.concept_id
            WHERE observation_concept_id > 0
            GROUP BY observation_concept_id;",
    cdm_database_schema = cdmDatabaseSchema,
    snakeCaseToCamelCase = TRUE
  ) |>
    dplyr::tibble()
  
  observationSourceCount <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT observation_source_concept_id source_concept_id,
              	COUNT(DISTINCT person_id) persons,
              	COUNT(*) records
              FROM @cdm_database_schema.observation dt
              INNER JOIN #cohort_index ct
            	ON dt.person_id = ct.subject_id
            		AND dt.observation_date = ct.cohort_start_date
              INNER JOIN #index_concepts_2 ic
              ON observation_source_concept_id = ic.concept_id
              WHERE observation_source_concept_id != observation_concept_id
                AND observation_source_concept_id > 0
              GROUP BY observation_source_concept_id;",
      cdm_database_schema = cdmDatabaseSchema,
      snakeCaseToCamelCase = TRUE
    ) |>
    dplyr::tibble()
  
  conditionCount <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT condition_concept_id concept_id,
          	COUNT(DISTINCT person_id) persons,
          	COUNT(*) records
          FROM @cdm_database_schema.condition_occurrence dt
          INNER JOIN #cohort_index ct
        	ON dt.person_id = ct.subject_id
        		AND dt.condition_start_date = ct.cohort_start_date
          INNER JOIN #index_concepts_2 ic
          ON condition_concept_id = ic.concept_id
          WHERE condition_concept_id > 0
          GROUP BY condition_concept_id;",
    cdm_database_schema = cdmDatabaseSchema,
    snakeCaseToCamelCase = TRUE
  ) |>
    dplyr::tibble()
  
  conditionSourceCount <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT condition_source_concept_id source_concept_id,
              	COUNT(DISTINCT person_id) persons,
              	COUNT(*) records
              FROM @cdm_database_schema.condition_occurrence dt
              INNER JOIN #cohort_index ct
             	ON dt.person_id = ct.subject_id
             		AND dt.condition_start_date = ct.cohort_start_date
              INNER JOIN #index_concepts_2 ic
              ON condition_source_concept_id = ic.concept_id
              WHERE condition_source_concept_id != condition_concept_id
                AND condition_source_concept_id > 0
              GROUP BY condition_source_concept_id;",
      cdm_database_schema = cdmDatabaseSchema,
      snakeCaseToCamelCase = TRUE
    ) |>
    dplyr::tibble()
  
  measurementCount <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT measurement_concept_id concept_id,
            	COUNT(DISTINCT person_id) persons,
            	COUNT(*) records
            FROM @cdm_database_schema.measurement dt
            INNER JOIN #cohort_index ct
           	ON dt.person_id = ct.subject_id
           		AND dt.measurement_date = ct.cohort_start_date
            INNER JOIN #index_concepts_2 ic
            ON measurement_concept_id = ic.concept_id
            WHERE measurement_concept_id > 0
            GROUP BY measurement_concept_id;",
    cdm_database_schema = cdmDatabaseSchema,
    snakeCaseToCamelCase = TRUE
  ) |>
    dplyr::tibble()
  
  measurementSourceCount <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT measurement_source_concept_id source_concept_id,
            	COUNT(DISTINCT person_id) persons,
            	COUNT(*) records
            FROM @cdm_database_schema.measurement dt
            INNER JOIN #cohort_index ct
          	ON dt.person_id = ct.subject_id
          		AND dt.measurement_date = ct.cohort_start_date
            INNER JOIN #index_concepts_2 ic
            ON measurement_source_concept_id = ic.concept_id
            WHERE measurement_source_concept_id != measurement_concept_id
              AND measurement_source_concept_id > 0
            GROUP BY measurement_source_concept_id;",
      cdm_database_schema = cdmDatabaseSchema,
      snakeCaseToCamelCase = TRUE
    ) |>
    dplyr::tibble()
  
  
  browser()
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = " DROP TABLE IF EXISTS #cohort_index;
            DROP TABLE IF EXISTS #index_concepts_2;"
  )
  
  output <- dplyr::bind_rows(
    visitCount |> dplyr::mutate(source = "v1"),
    visitSourceCount |>
      dplyr::anti_join(visitCount |>
                         dplyr::select(conceptId) |>
                         dplyr::distinct()) |>
      dplyr::mutate(source = "v2"),
    procedureCount |> dplyr::mutate(source = "p1"),
    procedureSourceCount |>
      dplyr::anti_join(
        procedureCount |>
          dplyr::select(conceptId) |>
          dplyr::distinct()
      ) |>
      dplyr::mutate(source = "p2"),
    drugExposureCount |> dplyr::mutate(source = "d1"),
    drugExposureSourceCount |>
      dplyr::anti_join(
        drugExposureCount |>
          dplyr::select(conceptId) |>
          dplyr::distinct()
      ) |>
      dplyr::mutate(source = "d2"),
    observationCount |> dplyr::mutate(source = "o1"),
    observationSourceCount |>
      dplyr::anti_join(
        observationCount |>
          dplyr::select(conceptId) |>
          dplyr::distinct()
      ) |>
      dplyr::mutate(source = "o2"),
    conditionCount |> dplyr::mutate(source = "c1"),
    conditionSourceCount |>
      dplyr::anti_join(
        conditionCount |>
          dplyr::select(conceptId) |>
          dplyr::distinct()
      ) |>
      dplyr::mutate(source = "c2"),
    measurementCount |> dplyr::mutate(source = "m1"),
    measurementSourceCount |>
      dplyr::anti_join(
        measurementCount |>
          dplyr::select(conceptId) |>
          dplyr::distinct()
      ) |>
      dplyr::mutate(source = "m2")
  )
  
  return(output)
  
}