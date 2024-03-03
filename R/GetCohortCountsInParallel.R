#' @export
getCohortCountsInParallel <- function(cdmSources = NULL,
                                      cohortTableName,
                                      sequence = 1,
                                      databaseIds = NULL,
                                      cohortIds = NULL) {
  
  cdmSources <-
    getCdmSource(cdmSources = cdmSources,
                 database = databaseIds,
                 sequence = sequence)

  if (!is.null(cohortIds)) {
    sql <- "
        SELECT  cohort_definition_id,
                count(*) cohort_entries,
                count(DISTINCT subject_id) cohort_subjects
        FROM @cohort_database_schema.@cohort_table_name
        WHERE cohort_definition_id IN (@cohort_ids)
        GROUP BY cohort_definition_id;"

    cohortCounts <-
      OhdsiHelpers::renderTranslateQuerySqlInParallel(
        cdmSources = cdmSources,
        sql = sql,
        cohort_ids = cohortIds,
        cohort_table_name = cohortTableName,
        database_id = databaseIds
      )
  } else {
    sql <- "
        SELECT  cohort_definition_id,
                count(*) cohort_entries,
                count(DISTINCT subject_id) cohort_subjects
        FROM @cohort_database_schema.@cohort_table_name
        GROUP BY cohort_definition_id;"

    cohortCounts <-
      OhdsiHelpers::renderTranslateQuerySqlInParallel(
        cdmSources = cdmSources,
        sql = sql,
        cohort_table_name = cohortTableName,
        databaseIds = databaseIds
      )
  }

  results <- c()
  results$cohortCounts <- cohortCounts |>
    dplyr::rename(cohortId = .data$cohortDefinitionId) |>
    dplyr::arrange(
      .data$cohortId,
      .data$databaseKey
    ) |>
    dplyr::rename(sourceKey = .data$databaseKey)

  results$databaseId$cohortSubjects <-
    results$cohortCounts |>
    dplyr::select(
      .data$cohortId,
      .data$cohortSubjects,
      .data$databaseId
    ) |>
    tidyr::pivot_wider(
      id_cols = c("cohortId"),
      names_from = "databaseId",
      values_from = "cohortSubjects",
      values_fill = 0
    ) |>
    dplyr::arrange(.data$cohortId)

  results$databaseId$cohortEntries <-
    results$cohortCounts |>
    dplyr::select(
      .data$cohortId,
      .data$cohortEntries,
      .data$databaseId
    ) |>
    tidyr::pivot_wider(
      id_cols = c("cohortId"),
      names_from = "databaseId",
      values_from = "cohortEntries",
      values_fill = 0
    ) |>
    dplyr::arrange(.data$cohortId)

  results$sourceKey$cohortSubjects <-
    results$cohortCounts |>
    dplyr::select(
      .data$cohortId,
      .data$cohortSubjects,
      .data$sourceKey
    ) |>
    tidyr::pivot_wider(
      id_cols = c("cohortId"),
      names_from = "sourceKey",
      values_from = "cohortSubjects",
      values_fill = 0
    ) |>
    dplyr::arrange(.data$cohortId)

  results$sourceKey$cohortEntries <-
    results$cohortCounts |>
    dplyr::select(
      .data$cohortId,
      .data$cohortEntries,
      .data$sourceKey
    ) |>
    tidyr::pivot_wider(
      id_cols = c("cohortId"),
      names_from = "sourceKey",
      values_from = "cohortEntries",
      values_fill = 0
    ) |>
    dplyr::arrange(.data$cohortId)

  return(results)
}
