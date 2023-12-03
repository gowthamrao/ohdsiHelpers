#' @export
#'
saveCandidateCohortData <- function(connectionDetails,
                                    cohortDatabaseSchema,
                                    cohortTable,
                                    candidateCohortId,
                                    resultsFolder) {
  candidateCohortData <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = DatabaseConnector::connect(connectionDetails = connectionDetails),
      sql = " SELECT * FROM @cohort_database_schema.@cohort_table
              WHERE cohort_definition_id IN (@candidate_cohort_ids)
              ORDER BY cohort_definition_id, subject_id, cohort_start_date, cohort_end_date;",
      cohort_database_schema = cohortDatabaseSchema,
      cohort_table = cohortTable,
      candidate_cohort_ids = candidateCohortId,
      snakeCaseToCamelCase = TRUE
    ) %>% dplyr::tibble()

  saveRDS(
    object = candidateCohortData,
    file = file.path(
      resultsFolder,
      "candidateCohortData.rds"
    )
  )
}
