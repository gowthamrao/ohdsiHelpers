#' @export
getCohortIncStatsInParallel <- function(cdmSources = NULL,
                                        cohortTableNames,
                                        sequence = 1,
                                        databaseIds = NULL,
                                        cohortIds = NULL) {
  
  if (is.null(cohortTableNames$cohortInclusionStatsTable)) {
    stop("'cohortInclusionStatsTable' not found in cohortTableNames")
  }
  
  cdmSources <-
    getCdmSource(cdmSources = cdmSources,
                 database = databaseIds,
                 sequence = sequence)
  
  if (!is.null(cohortIds)) {
    sql <- "
        SELECT  *
        FROM @cohort_database_schema.@cohort_table_name
        WHERE cohort_definition_id IN (@cohort_ids);"
    
    output <-
      OhdsiHelpers::renderTranslateQuerySqlInParallel(
        cdmSources = cdmSources,
        sql = sql,
        cohort_ids = cohortIds,
        cohort_table_name = cohortTableNames$cohortInclusionStatsTable,
        database_id = databaseIds
      )
  } else {
    sql <- "
        SELECT  *
        FROM @cohort_database_schema.@cohort_table_name;"
    
    output <-
      OhdsiHelpers::renderTranslateQuerySqlInParallel(
        cdmSources = cdmSources,
        sql = sql,
        cohort_table_name = cohortTableNames$cohortInclusionStatsTable,
        databaseIds = databaseIds
      )
  }
  
  return(output)
}
