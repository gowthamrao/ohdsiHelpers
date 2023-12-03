#' @export
getDrugExposureUtilizationStrata <-
  function(stratifyBySexSpecifications,
           stratifyByAgeSpecifications,
           stratifyByDateRangeSpecifications) {
    stratifyBySex <- stratifyBySexSpecifications |>
      dplyr::rename(idSex = .data$id) |>
      dplyr::distinct()

    stratifyByAge <- stratifyByAgeSpecifications |>
      dplyr::rename(idAge = .data$id) |>
      dplyr::distinct()

    stratifyByDateRange <- stratifyByDateRangeSpecifications |>
      dplyr::rename(idCalendarYear = .data$id) |>
      dplyr::distinct()

    reportingStrata <- tidyr::crossing(
      stratifyBySex,
      stratifyByDateRange,
      stratifyByAge
    ) |>
      dplyr::mutate(strataId = (.data$idSex * 100) +
        (.data$iidAge * 10000) +
        (.data$iidCalendarYear * 1000000)) |>
      dplyr::filter(.data$strataId > 0) |>
      dplyr::select(
        .data$strataId,
        .data$genderConceptId,
        .data$dateOnOrAfter,
        .data$dateBefore,
        .data$ageGreaterThanOrEqualTo,
        .data$ageLowerThan
      )

    return(reportingStrata)
  }


#' @export
getDrugExposureUtilizationSummary <- function(connection = NULL,
                                              connectionDetails = NULL,
                                              cdmDatabaseSchema,
                                              vocabularyDatabaseSchema = cdmDatabaseSchema,
                                              drugConceptIds,
                                              includeDescendants = TRUE,
                                              reportingStrata = getDrugExposureUtilizationStrata(),
                                              tempEmulationSchema = NULL) {
  # Set up connection to server ----------------------------------------------------
  if (is.null(connection)) {
    if (!is.null(connectionDetails)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    } else {
      stop("No connection or connectionDetails provided.")
    }
  }

  conceptIds <- drugConceptIds
  if (includeDescendants) {
    conceptIds <-
      ConceptSetDiagnostics::getConceptDescendant(
        connection = connection,
        conceptIds = conceptIds,
        vocabularyDatabaseSchema = vocabularyDatabaseSchema
      )
    conceptIds <- conceptIds$descendantConceptId |> unique()
  }

  conceptIdTableName <-
    ConceptSetDiagnostics:::loadTempConceptTable(
      conceptIds = conceptIds,
      connection = connection
    )

  tempTableName <-
    paste0("#", ConceptSetDiagnostics:::getUniqueString())

  invisible(utils::capture.output(
    DatabaseConnector::insertTable(
      connection = connection,
      tableName = tempTableName,
      dropTableIfExists = TRUE,
      tempTable = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      data = reportingStrata,
      camelCaseToSnakeCase = TRUE,
      progressBar = TRUE,
      createTable = TRUE
    ),
    file = nullfile()
  ))

  sql <-
    SqlRender::loadRenderTranslateSql(
      sqlFilename = "DrugUtilizationStratified.sql",
      packageName = utils::packageName(),
      dbms = connection@dbms,
      tempEmulationSchema = tempEmulationSchema,
      cdm_database_schema = cdmDatabaseSchema,
      strata_table = tempTableName,
      concept_id_table = conceptIdTableName
    )
  DatabaseConnector::executeSql(connection, sql)

  numeratorCount <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = "SELECT * FROM #numerator_counts;",
    snakeCaseToCamelCase = TRUE,
    tempEmulationSchema = tempEmulationSchema
  ) |>
    dplyr::tibble()

  # denominatorCount <- DatabaseConnector::renderTranslateQuerySql(
  #   connection = connection,
  #   sql = "SELECT * FROM #denominator_counts;",
  #   snakeCaseToCamelCase = TRUE,
  #   tempEmulationSchema = tempEmulationSchema
  # ) |>
  #   dplyr::tibble()

  # strataReduced <- reportingStrata |>
  #   dplyr::group_by(strataId) |>
  #   dplyr::mutate(
  #     ageGreaterThanOrEqualTo = paste0(ageGreaterThanOrEqualTo, collapse = ", "),
  #     ageLowerThan = paste0(ageLowerThan, collapse = ", "),
  #     genderConceptId = paste0(genderConceptId, collapse = ", "),
  #     dateOnOrAfter = paste0(dateOnOrAfter, collapse = ", "),
  #     dateBefore = paste0(dateBefore, collapse = ", ")
  #   ) |>
  #   dplyr::distinct() |>
  #   dplyr::ungroup()

  # results <- dplyr::bind_rows(
  #   numeratorCount |>
  #     dplyr::select(strataId),
  #   denominatorCount |>
  #     dplyr::select(strataId)
  # ) |>
  #   dplyr::distinct() |>
  #   dplyr::left_join(numeratorCount,
  #                    by = "strataId") |>
  #   dplyr::left_join(denominatorCount,
  #                    by = "strataId") |>
  #
  #   dplyr::mutate(eventsPerPersonWithEvent = (numeratorRecordCount / numeratorPersonCount)) |>
  #   dplyr::mutate(datesPerPersonWithEvent = (numeratorDateCount / numeratorPersonCount)) |>
  #
  #   dplyr::mutate(eventsPer1KEligible = (numeratorRecordCount / denominatorPersonCount) * 1000) |>
  #   dplyr::mutate(personsPer1KEligible = (numeratorPersonCount / denominatorPersonCount) * 1000) |>
  #   dplyr::mutate(datesPer1KEligible = (numeratorDateCount / denominatorPersonCount) * 1000) |>
  #
  #   dplyr::mutate(
  #     eventsPer1KEligiblePY = (numeratorRecordCount / denominatorPersonDays) * 365.25 * 1000
  #   ) |>
  #   dplyr::mutate(
  #     personsPer1KEligiblePY = (numeratorPersonCount / denominatorPersonDays) * 365.25 * 1000
  #   ) |>
  #   dplyr::mutate(datesPer1KPY = (numeratorDateCount / denominatorPersonDays) * 365.25 * 1000) |>
  #   dplyr::tibble()

  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = " DROP TABLE IF EXISTS #numerator_counts;
            DROP TABLE IF EXISTS #denominator_counts;",
    tempEmulationSchema = tempEmulationSchema
  )

  return(numeratorCount)
}
