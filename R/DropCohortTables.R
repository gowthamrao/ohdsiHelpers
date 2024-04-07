#' @export
dropCohortTablesInParallel <- function(cdmSources,
                                       cohortTableNames) {
  sql <- "DROP TABLE IF EXISTS @cohort_database_schema.@cohort_table;

          DROP TABLE IF EXISTS @cohort_database_schema.@cohort_inclusion_table;

          DROP TABLE IF EXISTS @cohort_database_schema.@cohort_inclusion_result_table;

          DROP TABLE IF EXISTS @cohort_database_schema.@cohort_inclusion_stats_table;

          DROP TABLE IF EXISTS @cohort_database_schema.@cohort_summary_stats_table;

          DROP TABLE IF EXISTS @cohort_database_schema.@cohort_censor_stats_table;"


  OhdsiHelpers::renderTranslateExecuteSqlInParallel(
    cdmSources = OhdsiHelpers::getCdmSource(cdmSources = cdmSources),
    sql = sql,
    cohort_table = cohortTableNames$cohortTable,
    cohort_inclusion_table = cohortTableNames$cohortInclusionTable,
    cohort_inclusion_result_table = cohortTableNames$cohortInclusionResultTable,
    cohort_inclusion_stats_table = cohortTableNames$cohortInclusionStatsTable,
    cohort_summary_stats_table = cohortTableNames$cohortSummaryStatsTable,
    cohort_censor_stats_table = cohortTableNames$cohortCensorStatsTable
  )
}
