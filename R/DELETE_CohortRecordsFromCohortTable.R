#' @export
deleteCohortRecordsFromCohortTableInParallel <-
  function(cohortTableNames,
           cdmSources,
           databaseIds,
           deleteCohortIds = NULL,
           keepCohortIds = NULL) {
    if (all(!is.null(deleteCohortIds),!is.null(keepCohortIds))) {
      stop("cannot give both keep and delete cohort ids")
    }
    if (all(is.null(deleteCohortIds),
            is.null(keepCohortIds))) {
      stop("please give one of keep and delete cohort ids")
    }
    
    if (!is.null(keepCohortIds)) {
      sql <- "
            DELETE FROM @cohort_database_schema.@cohort_table_name
            WHERE cohort_definition_id NOT IN (@cohort_ids_to_keep);

            UPDATE STATISTICS @cohort_database_schema.@cohort_table_name;"
      
      OhdsiHelpers::renderTranslateExecuteSqlInParallel(
        cdmSources = cdmSources,
        sql = sql,
        cohort_table_name = cohortTableNames$cohortTable,
        cohort_ids_to_keep = keepCohortIds,
        databaseIds = databaseIds
      )
    }
    
    if (!is.null(deleteCohortIds)) {
      sql <- "
            DELETE FROM @cohort_database_schema.@cohort_table_name
            WHERE cohort_definition_id IN (@cohort_ids_to_delete);

            UPDATE STATISTICS @cohort_database_schema.@cohort_table_name;"
      
      OhdsiHelpers::renderTranslateExecuteSqlInParallel(
        cdmSources = cdmSources,
        sql = sql,
        cohort_table_name = cohortTableNames$cohortTable,
        cohort_ids_to_delete = deleteCohortIds,
        databaseIds = databaseIds
      )
    }
  }
