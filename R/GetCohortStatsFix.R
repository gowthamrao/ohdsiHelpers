#' @export
getCohortStatsFix <- function(connectionDetails,
                              connection = NULL,
                              cohortDatabaseSchema,
                              databaseId = NULL,
                              snakeCaseToCamelCase = TRUE,
                              outputTables = c(
                                "cohortInclusionTable",
                                "cohortInclusionResultTable",
                                "cohortInclusionStatsTable",
                                "cohortInclusionStatsTable",
                                "cohortSummaryStatsTable",
                                "cohortCensorStatsTable"
                              ),
                              cohortTableNames) {
  # Names of cohort table names must include output tables
  checkmate::assertNames(names(cohortTableNames), must.include = outputTables)
  # ouput tables strictly the set of allowed tables
  checkmate::assertNames(
    outputTables,
    subset.of = c(
      "cohortInclusionTable",
      "cohortInclusionResultTable",
      "cohortInclusionStatsTable",
      "cohortInclusionStatsTable",
      "cohortSummaryStatsTable",
      "cohortCensorStatsTable"
    )
  )
  results <- list()
  
  for (table in outputTables) {
    # The cohortInclusionTable does not hold database
    # specific information so the databaseId
    # should NOT be included.
    includeDatabaseId <-
      ifelse(
        test = table != "cohortInclusionTable",
        yes = TRUE,
        no = FALSE
      )
    results[[table]] <- CohortGenerator:::getStatsTable(
      connectionDetails = connectionDetails,
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      table = cohortTableNames[[table]],
      databaseId = databaseId,
      snakeCaseToCamelCase = snakeCaseToCamelCase,
      includeDatabaseId = includeDatabaseId
    )
  }
  return(results)
}