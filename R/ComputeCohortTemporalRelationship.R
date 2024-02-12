#' @export
computeCohortTemporalRelationship <- function(connection = NULL,
                                              connectionDetails = NULL,
                                              cohortDatabaseSchema,
                                              cohortTableName,
                                              eventCohortIds,
                                              targetCohortIds,
                                              maxDays,
                                              roundDays = 1,
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
  
  sql <-
    SqlRender::loadRenderTranslateSql(
      sqlFilename = "ComputeCohortTemporalRelationship.sql",
      packageName = utils::packageName(),
      dbms = connection@dbms,
      tempEmulationSchema = tempEmulationSchema,
      cohort_database_schema = cohortDatabaseSchema,
      cohort_table = cohortTableName,
      max_days_diff = maxDays,
      round_days = roundDays,
      target_cohort_definition_ids = targetCohortIds,
      event_cohort_definition_ids = eventCohortIds
    )
  results <-
    DatabaseConnector::querySql(
      connection = connection,
      sql = sql,
      snakeCaseToCamelCase = TRUE
    ) |>
    dplyr::tibble()
  
  return(results)
}
