#' @export
timeToFeature <- function(targetCohortIds,
                          featureCohortIds,
                          allDatabaseIds,
                          targetCohortTableName,
                          featureCohortTableName = targetCohortTableName,
                          cdmSources,
                          plotTitle = paste0("Time to cohort ids: ",
                                             paste0(featureCohortIds, collapse = ", ")),
                          minDays = NULL,
                          maxDays = NULL,
                          tempEmulationationSchema = getOption("sqlRenderTempEmulationSchema")) {
  
  sql <- 
    "
      with target_cohort as
      (
              SELECT subject_id,
                      target_start_date,
                      row_number() over(partition by subject_id order by target_start_date) target_sequence
              FROM
              (
                  SELECT DISTINCT subject_id,
                          cohort_start_date target_start_date
                  FROM @cohort_database_schema.@target_cohort_table
                  WHERE cohort_definition_id IN (@target_cohort_ids)
              ) t1
      )
      SELECT target_sequence,
              DATEDIFF(day, target_start_date, feature_start_date) AS days_to_feature,
              COUNT(DISTINCT subject_id) subjects
      FROM (
              SELECT f1.subject_id,
                        t1.target_sequence,
                        t1.target_start_date,
                        min(f1.cohort_start_date) feature_start_date
              FROM @cohort_database_schema.@feature_cohort_table f1
              INNER JOIN target_cohort t1
              ON  t1.subject_id = f1.subject_id
                AND t1.target_start_date <= f1.cohort_start_date
              WHERE f1.cohort_definition_id IN (@feature_cohort_ids)
              GROUP BY f1.subject_id,
                        t1.target_sequence,
                        t1.target_start_date
            ) f
      WHERE target_sequence > 0
            {@min_days_is_not_null} ? {
              AND DATEDIFF(day, target_start_date, feature_start_date) >= @min_days
            }
            {@max_days_is_not_null} ? {
              AND DATEDIFF(day, target_start_date, feature_start_date) <= @max_days
            }
      GROUP BY target_sequence,
              DATEDIFF(day, target_start_date, feature_start_date);
        "
  timeToFeatureCohort <-
    OhdsiHelpers::renderTranslateQuerySqlInParallel(
      cdmSources = cdmSources,
      databaseIds = allDatabaseIds,
      sequence = 1,
      sql = sql,
      target_cohort_table = targetCohortTableName,
      feature_cohort_table = featureCohortTableName,
      target_cohort_ids = targetCohortIds,
      feature_cohort_ids = featureCohortIds,
      tempEmulationSchema = tempEmulationSchema,
      min_days = minDays,
      max_days = maxDays,
      min_days_is_not_null = !is.null(minDays),
      max_days_is_not_null = !is.null(maxDays)
    )
  
  longData <- timeToFeatureCohort |>
    dplyr::filter(targetSequence == 1) |>
    dplyr::rename(value = daysToFeature,
                  groupLabel = databaseId) |>
    dplyr::select(value, groupLabel, subjects) |>
    tidyr::uncount(weights = subjects) |>
    dplyr::arrange(groupLabel, value) |>
    dplyr::relocate(groupLabel, value)
  
  violinPlots <-
    OhdsiPlots::createViolinPlot(
      data = longData,
      title = plotTitle)
  
  summaryStatistics <-
    longData |>
    OhdsiHelpers::calculateSummaryStatistics(group = 'groupLabel') |>
    dplyr::mutate(value = OhdsiHelpers::formatDecimalWithComma(number = value)) |>
    tidyr::pivot_wider(id_cols = "statistic",
                       values_from = value,
                       names_from = groupLabel) |>
    dplyr::arrange(statistic)
  
  output <- c()
  
  output$summaryStatistics <- summaryStatistics
  output$violinPlot <- violinPlots
  return(output)
}