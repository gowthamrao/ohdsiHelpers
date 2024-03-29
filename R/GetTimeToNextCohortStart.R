#' @export
getTimeToNextCohortStart <- function(connectionDetails = NULL,
                                     connection = NULL,
                                     targetCohortDatabaseSchema,
                                     targetCohortTableName,
                                     targetCohortId,
                                     featureCohortId = targetCohortId,
                                     featureCohortDatabaseSchema = targetCohortDatabaseSchema,
                                     featureCohortTableName = targetCohortTableName,
                                     tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }



  sql <- "SELECT subject_id,
          	DATEDIFF(day, cohort_start_date, next_start_date) AS days_to_next,
          	count(DISTINCT cohort_start_date) records
          FROM (
          	SELECT subject_id,
          		cohort_start_date,
          		next_start_date,
          		ROW_NUMBER() OVER (
          			PARTITION BY subject_id,
          			cohort_start_date 
          			ORDER BY next_start_date
          			) rn
          	FROM (
          		SELECT DISTINCT t.subject_id,
          			t.cohort_start_date,
          			LEAD(f.cohort_start_date) OVER (
          				PARTITION BY t.subject_id,
          				t.cohort_start_date 
          				ORDER BY f.cohort_start_date
          				) AS next_start_date
          		FROM (
          			SELECT subject_id, 
          			        cohort_start_date
          			FROM @target_cohort_database_schema.@target_cohort_table_name
          			WHERE cohort_definition_id = @target_cohort_definition_id
          			) t
          		INNER JOIN (
          			SELECT subject_id,
          				cohort_start_date
          			FROM @feature_cohort_database_schema.@feature_cohort_table_name
          			WHERE cohort_definition_id = @feature_cohort_definition_id
          			) AS f
          			ON t.subject_id = f.subject_id
          			AND t.cohort_start_date <= f.cohort_start_date
          		) f
          	WHERE DATEDIFF(day, cohort_start_date, next_start_date) IS NOT NULL
          	) f2
          WHERE rn = 1
          GROUP BY subject_id,
          	DATEDIFF(day, cohort_start_date, next_start_date)
          ORDER BY subject_id,
          	DATEDIFF(day, cohort_start_date, next_start_date)
"
  subjectRecords <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = sql,
    snakeCaseToCamelCase = TRUE,
    tempEmulationSchema = tempEmulationSchema,
    target_cohort_database_schema = targetCohortDatabaseSchema,
    target_cohort_table_name = targetCohortTableName,
    target_cohort_definition_id = targetCohortId,
    feature_cohort_database_schema = featureCohortDatabaseSchema,
    feature_cohort_table_name = featureCohortTableName,
    feature_cohort_definition_id = featureCohortId
  ) |> 
    dplyr::tibble()

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
    )|> 
    dplyr::ungroup() 

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
