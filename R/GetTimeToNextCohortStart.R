#' @export
getTimeToNextCohortStart <- function(connectionDetails = NULL,
                                     connection = NULL,
                                     cohortDatabaseSchema,
                                     cohortDefinitionId = NULL,
                                     cdmDatabaseSchema = NULL,
                                     cohortTableName,
                                     tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  if (!is.null(cohortDatabaseSchema)) {
    if (is.null(cohortDefinitionId)) {
      stop("cohortDatabaseSchema is not NULL, but cohortDefinitionId is NULL")
    }
  }

  sql <- "
  SELECT
    subject_id,
    DATEDIFF(day, cohort_start_date, next_start_date) AS days_to_next,
    count(*) records
FROM
    (SELECT
        subject_id,
        cohort_start_date,
        LEAD(cohort_start_date) OVER (PARTITION BY subject_id ORDER BY cohort_start_date) AS next_start_date
     FROM @cohort_database_schema.@cohort_table_name
     WHERE cohort_definition_id = @cohort_definition_id
    ) AS a
  GROUP BY subject_id, DATEDIFF(day, cohort_start_date, next_start_date)
  ORDER BY subject_id, DATEDIFF(day, cohort_start_date, next_start_date)
"
  subjectRecords <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = sql,
    snakeCaseToCamelCase = TRUE,
    tempEmulationSchema = tempEmulationSchema,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table_name = cohortTableName,
    cohort_definition_id = cohortDefinitionId
  )

  output <- c()
  output$daysToNextDistribution <- subjectRecords |>
    dplyr::group_by(.data$daysToNext) |>
    dplyr::summarize(
      totalRecords = sum(.data$records),
      uniqueSubjectIds = dplyr::n_distinct(.data$subjectId),
      minRecords = min(.data$records),
      maxRecords = max(.data$records),
      percentile1 = quantile(.data$records, probs = 0.01),
      percentile5 = quantile(.data$records, probs = 0.05),
      percentile10 = quantile(.data$records, probs = 0.10),
      percentile25 = quantile(.data$records, probs = 0.25),
      medianRecords = median(.data$records),
      percentile75 = quantile(.data$records, probs = 0.75),
      percentile90 = quantile(.data$records, probs = 0.90),
      percentile95 = quantile(.data$records, probs = 0.95),
      percentile99 = quantile(.data$records, probs = 0.99),
      meanRecords = mean(.data$records),
      sdRecords = sd(.data$records),
      iqrRecords = IQR(.data$records)
    )

  output$daysToNext <- subjectRecords |>
    dplyr::select(
      .data$subjectId,
      .data$daysToNext
    ) |>
    dplyr::filter(!is.na(.data$daysToNext)) |>
    dplyr::group_by(.data$subjectId) |>
    dplyr::summarize(records = dplyr::n_distinct(.data$daysToNext)) |>
    dplyr::ungroup() |>
    dplyr::summarize(
      totalRecords = sum(.data$records),
      uniqueSubjectIds = dplyr::n_distinct(.data$subjectId),
      minRecords = min(.data$records),
      maxRecords = max(.data$records),
      percentile1 = quantile(.data$records, probs = 0.01),
      percentile5 = quantile(.data$records, probs = 0.05),
      percentile10 = quantile(.data$records, probs = 0.10),
      percentile25 = quantile(.data$records, probs = 0.25),
      medianRecords = median(.data$records),
      percentile75 = quantile(.data$records, probs = 0.75),
      percentile90 = quantile(.data$records, probs = 0.90),
      percentile95 = quantile(.data$records, probs = 0.95),
      percentile99 = quantile(.data$records, probs = 0.99),
      meanRecords = mean(.data$records),
      sdRecords = sd(.data$records),
      iqrRecords = IQR(.data$records)
    ) |>
    dplyr::mutate(daysToNext = -1)

  output <- dplyr::bind_rows(
    output$daysToNextDistribution,
    output$daysToNext
  ) |>
    dplyr::arrange(.data$daysToNext)

  return(output)
}
