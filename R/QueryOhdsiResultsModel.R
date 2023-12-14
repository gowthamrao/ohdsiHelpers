#' @export
queryOhdsiResultsCohortCounts <-
  function(connectionDetails = NULL,
           connection = NULL,
           resultsSchema,
           cohortIds = NULL,
           databaseIds = NULL,
           prefix = NULL,
           cdmSources,
           model = "CohortDiagnostics") {
    if (is.null(connection)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    }
    output <- c()
    if (model == "CohortDiagnostics") {
      output$cohort <- DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = " SELECT * FROM @results_schema.@prefixcohort
                WHERE cohort_id IS NOT NULL
                {@cohort_ids != ''} ? {  AND cohort_id in (@cohort_ids)};",
        results_schema = resultsSchema,
        cohort_ids = cohortIds,
        prefix = prefix,
        snakeCaseToCamelCase = TRUE
      ) |> dplyr::tibble()
      
      output$cohortCount <-
        DatabaseConnector::renderTranslateQuerySql(
          connection = connection,
          sql = " SELECT * FROM @results_schema.@prefixcohort_count
                WHERE cohort_id IS NOT NULL
                {@use_database_ids} ? { AND database_id in (@database_ids)}
                {@cohort_ids != ''} ? {  AND cohort_id in (@cohort_ids)};",
          results_schema = resultsSchema,
          cohort_ids = cohortIds,
          use_database_ids = !is.null(databaseIds),
          database_ids = quoteLiterals(databaseIds),
          prefix = prefix,
          snakeCaseToCamelCase = TRUE
        ) |> dplyr::tibble()
      
      output$cohortInclusion <-
        DatabaseConnector::renderTranslateQuerySql(
          connection = connection,
          sql = " SELECT * FROM @results_schema.@prefixcohort_inclusion
                WHERE cohort_id IS NOT NULL
                {@use_database_ids} ? { AND database_id in (@database_ids)}
                {@cohort_ids != ''} ? {  AND cohort_id in (@cohort_ids)};",
          results_schema = resultsSchema,
          cohort_ids = cohortIds,
          use_database_ids = !is.null(databaseIds),
          database_ids = quoteLiterals(databaseIds),
          prefix = prefix,
          snakeCaseToCamelCase = TRUE
        ) |> dplyr::tibble()
      
      output$cohortIncResult <-
        DatabaseConnector::renderTranslateQuerySql(
          connection = connection,
          sql = " SELECT * FROM @results_schema.@prefixcohort_inc_result
                WHERE cohort_id IS NOT NULL
                {@use_database_ids} ? { AND database_id in (@database_ids)}
                {@cohort_ids != ''} ? {  AND cohort_id in (@cohort_ids)};",
          results_schema = resultsSchema,
          use_database_ids = !is.null(databaseIds),
          database_ids = quoteLiterals(databaseIds),
          cohort_ids = cohortIds,
          prefix = prefix,
          snakeCaseToCamelCase = TRUE
        ) |> dplyr::tibble()
      
      output$cohortIncStats <-
        DatabaseConnector::renderTranslateQuerySql(
          connection = connection,
          sql = " SELECT * FROM @results_schema.@prefixcohort_inc_stats
                WHERE cohort_id IS NOT NULL
                {@use_database_ids} ? { AND database_id in (@database_ids)}
                {@cohort_ids != ''} ? {  AND cohort_id in (@cohort_ids)};",
          results_schema = resultsSchema,
          use_database_ids = !is.null(databaseIds),
          database_ids = quoteLiterals(databaseIds),
          cohort_ids = cohortIds,
          prefix = prefix,
          snakeCaseToCamelCase = TRUE
        ) |> dplyr::tibble()
      
      output$cohortSummaryStats <-
        DatabaseConnector::renderTranslateQuerySql(
          connection = connection,
          sql = " SELECT * FROM @results_schema.@prefixcohort_summary_stats
                WHERE cohort_id IS NOT NULL
                {@use_database_ids} ? { AND database_id in (@database_ids)}
                {@cohort_ids != ''} ? {  AND cohort_id in (@cohort_ids)};",
          results_schema = resultsSchema,
          use_database_ids = !is.null(databaseIds),
          database_ids = quoteLiterals(databaseIds),
          cohort_ids = cohortIds,
          prefix = prefix,
          snakeCaseToCamelCase = TRUE
        ) |> dplyr::tibble()
      
      output$dataSources <-
        DatabaseConnector::renderTranslateQuerySql(
          connection = connection,
          sql = " SELECT * FROM @results_schema.@prefixdatabase
                WHERE database_id IS NOT NULL
                {@use_database_ids} ? { AND database_id in (@database_ids)};",
          results_schema = resultsSchema,
          use_database_ids = !is.null(databaseIds),
          database_ids = quoteLiterals(databaseIds),
          prefix = prefix,
          snakeCaseToCamelCase = TRUE
        ) |> 
        dplyr::tibble() |> 
        dplyr::select(-databaseId) |> 
        dplyr::left_join(cdmSources |> 
                           dplyr::filter(sequence == 1) |> 
                           dplyr::select(databaseId,
                                         databaseName,
                                         sourceName,
                                         database),
                         by = "databaseName")
      
      output$cohortCounts <- output$cohortCount |>
        dplyr::inner_join(output$dataSources |>
                            dplyr::select(databaseId,
                                          databaseName),
                          by = "databaseId") |>
        dplyr::inner_join(output$cohort |> dplyr::select("cohortName", "cohortId"),
                          by = "cohortId") |>
        dplyr::arrange(.data$cohortId, .data$databaseId) |> 
        dplyr::select(databaseId,
                      databaseName,
                      sourceName,
                      cohortId,
                      cohortName,
                      cohortEntries,
                      cohortSubjects
                      )
      
    } else {
      stop("unsupported model")
    }
    return(output)
  }