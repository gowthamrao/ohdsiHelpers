#' @export
getUnitConceptIdSummary <- function(connection = NULL,
                                    connectionDetails = NULL,
                                    cdmDatabaseSchema,
                                    measurementConceptIds,
                                    includeDescendants = TRUE,
                                    getPersonCount = FALSE) {
  # Set up connection to server ----------------------------------------------------
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      stop("No connection or connectionDetails provided.")
    }
  }
  
  conceptIds <- measurementConceptIds
  if (includeDescendants) {
    conceptIds <-
      ConceptSetDiagnostics::getConceptDescendant(
        connection = connection,
        conceptIds = measurementConceptIds,
        vocabularyDatabaseSchema = cdmSource$vocabDatabaseSchemaFinal
      )
    conceptIds <- conceptIds$descendantConceptId |> unique()
  }
  
  sql <- "
        DROP TABLE IF EXISTS #temp_1;
        DROP TABLE IF EXISTS #temp_2;
        DROP TABLE IF EXISTS #temp_3;
        DROP TABLE IF EXISTS #temp_4;
        DROP TABLE IF EXISTS #temp_5;

         SELECT measurement_concept_id,
        	measurement_type_concept_id,
        	unit_concept_id,
        	{@get_person_count} ? {count(DISTINCT person_id) person_count,}
        	count(DISTINCT measurement_id) record_count
        INTO #temp_1
        FROM @cdm_database_schema.measurement
        WHERE measurement_concept_id IN (@measurement_concepts)
        	AND measurement_concept_id != 0
        	AND measurement_type_concept_id != 0
        	AND measurement_concept_id IS NOT NULL
        	AND measurement_type_concept_id IS NOT NULL
        GROUP BY measurement_concept_id,
        	measurement_type_concept_id,
        	unit_concept_id;

        SELECT 0 measurement_concept_id,
        	measurement_type_concept_id,
        	unit_concept_id,
        	{@get_person_count} ? {count(DISTINCT person_id) person_count,}
        	count(DISTINCT measurement_id) record_count
        INTO #temp_2
        FROM @cdm_database_schema.measurement
        WHERE measurement_concept_id IN (@measurement_concepts)
        	AND measurement_concept_id != 0
        	AND measurement_type_concept_id != 0
        	AND measurement_concept_id IS NOT NULL
        	AND measurement_type_concept_id IS NOT NULL
        GROUP BY measurement_type_concept_id,
        	unit_concept_id;

        SELECT 0 measurement_concept_id,
        	0 measurement_type_concept_id,
        	unit_concept_id,
        	{@get_person_count} ? {count(DISTINCT person_id) person_count,}
        	count(DISTINCT measurement_id) record_count
        INTO #temp_3
        FROM @cdm_database_schema.measurement
        WHERE measurement_concept_id IN (@measurement_concepts)
        GROUP BY unit_concept_id;

        SELECT measurement_concept_id,
        	0 measurement_type_concept_id,
        	unit_concept_id,
        	{@get_person_count} ? {count(DISTINCT person_id) person_count,}
        	count(DISTINCT measurement_id) record_count
        INTO #temp_4
        FROM @cdm_database_schema.measurement
        WHERE measurement_concept_id IN (@measurement_concepts)
        	AND measurement_concept_id != 0
        	AND measurement_type_concept_id IS NOT NULL
        GROUP BY measurement_concept_id,
        	unit_concept_id;

        SELECT 0 measurement_concept_id,
        	0 measurement_type_concept_id,
        	unit_concept_id,
        	{@get_person_count} ? {count(DISTINCT person_id) person_count,}
        	count(DISTINCT measurement_id) record_count
        INTO #temp_5
        FROM @cdm_database_schema.measurement
        WHERE measurement_concept_id IN (@measurement_concepts)
        GROUP BY unit_concept_id;
  "
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    cdm_database_schema = cdmDatabaseSchema,
    measurement_concepts = conceptIds,
    get_person_count = getPersonCount
  )
  
  temp1 <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT * FROM #temp_1;",
    snakeCaseToCamelCase = TRUE
  )
  temp2 <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT * FROM #temp_2;",
    snakeCaseToCamelCase = TRUE
  )
  temp3 <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT * FROM #temp_3;",
    snakeCaseToCamelCase = TRUE
  )
  temp4 <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT * FROM #temp_4;",
    snakeCaseToCamelCase = TRUE
  )
  temp5 <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT * FROM #temp_5;",
    snakeCaseToCamelCase = TRUE
  )
  
  recordCount <- dplyr::bind_rows(temp1,
                                  temp2,
                                  temp3,
                                  temp4,
                                  temp5) |>
    dplyr::tibble() |>
    dplyr::arrange(measurementConceptId,
                   measurementTypeConceptId,
                   unitConceptId)
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    profile = FALSE,
    progressBar = FALSE,
    sql = "
    DROP TABLE IF EXISTS #temp_1;
    DROP TABLE IF EXISTS #temp_2;
    DROP TABLE IF EXISTS #temp_3;
    DROP TABLE IF EXISTS #temp_4;
    DROP TABLE IF EXISTS #temp_5;",
  )
  
  conceptIdDetails <- ConceptSetDiagnostics::getConceptIdDetails(
    conceptIds = c(
      recordCount$unitConceptId,
      recordCount$measurementConceptId,
      recordCount$measurementTypeConceptId
    ) |> unique(),
    connection = connection,
    vocabularyDatabaseSchema = cdmSource$vocabDatabaseSchemaFinal
  )
  
  output <- c()
  output$recordCount <- recordCount |>
    dplyr::arrange(dplyr::desc(recordCount))
  output$conceptIdDetails <- conceptIdDetails |>
    dplyr::arrange(conceptId)
  
  output$formattedOutput <- recordCount |>
    dplyr::inner_join(
      conceptIdDetails |>
        dplyr::select(conceptId,
                      conceptName) |>
        dplyr::rename(
          measurementConceptId = conceptId,
          measurementConceptName = conceptName
        ),
      by = "measurementConceptId"
    ) |>
    dplyr::inner_join(
      conceptIdDetails |>
        dplyr::select(conceptId,
                      conceptName) |>
        dplyr::rename(
          measurementTypeConceptId = conceptId,
          measurementTypeConceptName = conceptName
        ),
      by = "measurementTypeConceptId"
    ) |>
    dplyr::inner_join(
      conceptIdDetails |>
        dplyr::select(conceptId,
                      conceptName) |>
        dplyr::rename(unitConceptId = conceptId,
                      unitConceptName = conceptName),
      by = "unitConceptId"
    ) |>
    dplyr::relocate(
      measurementConceptId,
      measurementConceptName,
      measurementTypeConceptId,
      measurementTypeConceptName,
      unitConceptId,
      unitConceptName
    ) |>
    dplyr::arrange(dplyr::desc(recordCount))
  
  return(output)
}
