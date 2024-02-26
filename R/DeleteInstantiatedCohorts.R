#' @export
deleteInstantiatedCohortsInParrallel <- function(cdmSources = NULL,
                                                 cohortIdsToKeep = NULL,
                                                 cohortIdsToDelete = NULL,
                                                 cohortTableNames,
                                                 databaseIds) {
  if (!is.null(cohortIdsToKeep) && !is.null(cohortIdsToDelete)) {
    stop("pick one - either delete or keep. cant do both")
  }

  cdmSources <- cdmSources |>
    dplyr::filter(.data$database %in% c(databaseIds)) |>
    dplyr::filter(.data$sequence == 1)

  if (!is.null(cohortIdsToKeep)) {
    sql <- "
        DELETE FROM @cohort_database_schema.@cohort_table_name
        WHERE cohort_definition_id NOT IN (@cohort_ids);

        UPDATE STATISTICS @cohort_database_schema.@cohort_table_name;"

    cohortIds <- cohortIdsToKeep
  } else {
    sql <- "
        DELETE FROM @cohort_database_schema.@cohort_table_name
        WHERE cohort_definition_id IN (@cohort_ids);

        UPDATE STATISTICS @cohort_database_schema.@cohort_table_name;"
    cohortIds <- cohortIdsToDelete
  }

  OhdsiHelpers::renderTranslateExecuteSqlInParallel(
    cdmSources = cdmSources,
    sql = sql,
    cohort_ids = cohortIdsToKeep,
    cohort_table_name = cohortTableNames$cohortTable
  )
}
