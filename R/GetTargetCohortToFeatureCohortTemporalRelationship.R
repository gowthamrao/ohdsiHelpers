#' #' Function to calculate cohort periods
#' #' @description
#' #' Function to calculate cohort periods
#' #' @param connectionDetails Connection details, NULL by default
#' #' @param connection Database connection, NULL by default
#' #' @param targetCohortDatabaseSchema Database schema for target cohort
#' #' @param featureCohortDatabaseSchema Database schema for feature cohort, default to targetCohortDatabaseSchema
#' #' @param targetCohortTableName Table name for target cohort
#' #' @param featureCohortTableName Table name for feature cohort, default to targetCohortTableName
#' #' @param targetCohortId ID for target cohort
#' #' @param featureCohortIds IDs for feature cohorts
#' #' @param returnCohortData Boolean to decide if cohort data should be returned, TRUE by default
#' #' @return A list of cohort data and relationships
#' #' @export
#' getTargetCohortToFeatureCohortTemporalRelationship <-
#'   function(connectionDetails = NULL,
#'            connection = NULL,
#'            targetCohortDatabaseSchema,
#'            featureCohortDatabaseSchema = targetCohortDatabaseSchema,
#'            targetCohortTableName,
#'            featureCohortTableName = targetCohortTableName,
#'            targetCohortId,
#'            featureCohortIds,
#'            returnCohortData = TRUE) {
#'     if (is.null(connection)) {
#'       connection <- DatabaseConnector::connect(connectionDetails)
#'       on.exit(DatabaseConnector::disconnect(connection))
#'     }
#'     
#'     output <- list()
#'     
#'     sqlQuery <-
#'       "with temporal_rel AS
#'         ( SELECT t.cohort_definition_id target_cohort_definition_id,
#'                   f.cohort_definition_id feature_cohort_definition_id,
#'                   t.subject_id,
#'                   t.cohort_start_date target_cohort_start_date,
#'                   t.cohort_end_date target_cohort_end_date,
#'                   f.cohort_start_date feature_cohort_start_date,
#'                   f.cohort_end_date feature_cohort_end_date,
#'                   DATEDIFF(dd, @alias_2.@alias_2_start_date, @alias_1.@alias_1_start_date) @output_name,
#'                   ROW_NUMBER() OVER(PARTITION BY t.cohort_definition_id,
#'                                                   f.cohort_definition_id,
#'                                                   t.subject_id
#'                                     ORDER By abs(DATEDIFF(dd, @alias_2.@alias_2_start_date, @alias_1.@alias_1_start_date))) rn
#'           FROM
#'             @target_cohort_database_schema.@target_cohort_table_name t
#'           INNER JOIN
#'             @feature_cohort_database_schema.@feature_cohort_table_name f
#'           ON t.cohort_definition_id != f.cohort_definition_id
#'               AND t.subject_id = f.subject_id
#'           WHERE t.cohort_definition_id IN (@target_cohort_id)
#'               AND f.cohort_definition_id IN (@feature_cohort_id)
#'               {@limit_to_positive} ? {
#'               AND DATEDIFF(dd, @alias_2.@alias_2_start_date, @alias_1.@alias_1_start_date) > 0
#'               }
#'         )
#'         SELECT a.target_cohort_definition_id,
#'                 a.feature_cohort_definition_id,
#'                 a.subject_id,
#'                 a.target_cohort_start_date,
#'                 a.target_cohort_end_date,
#'                 a.feature_cohort_start_date,
#'                 a.feature_cohort_end_date,
#'                 a.@output_name,
#'                 ISNULL(closest,0) @output_name_closest
#'           FROM temporal_rel a
#'           LEFT JOIN
#'             (
#'                 SELECT  aa.*,
#'                         1 closest
#'                 FROM temporal_rel aa
#'                 WHERE rn = 1
#'             ) b
#'           ON a.target_cohort_definition_id = b.target_cohort_definition_id AND
#'             a.feature_cohort_definition_id = b.feature_cohort_definition_id AND
#'             a.subject_id = b.subject_id AND
#'             a.target_cohort_start_date = b.target_cohort_start_date AND
#'             a.target_cohort_end_date = b.target_cohort_end_date AND
#'             a.feature_cohort_start_date = b.feature_cohort_start_date AND
#'             a.feature_cohort_end_date = b.feature_cohort_end_date
#'           ORDER BY a.target_cohort_definition_id,
#'                   a.feature_cohort_definition_id,
#'                   a.subject_id,
#'                   a.target_cohort_start_date,
#'                   a.target_cohort_end_date,
#'                   a.feature_cohort_start_date,
#'                   a.feature_cohort_end_date,
#'                   ISNULL(closest,0) DESC,
#'                   a.@output_name;"
#'     
#'     tableAndDates <-
#'       dplyr::tibble(tableFields = c("ts", "te", "fs", "fe"))
#'     
#'     tableAndDatesCombinations <- tableAndDates |>
#'       tidyr::crossing(tableFields2 = tableFields) |>
#'       dplyr::filter(tableFields != tableFields2) |>
#'       dplyr::mutate(
#'         outputName = paste0(tableFields, "_", tableFields2),
#'         alias1 = ifelse(stringr::str_detect(tableFields, "f"), "f", "t"),
#'         alias2 = ifelse(stringr::str_detect(tableFields2, "f"), "f", "t"),
#'         alias1StartDate = ifelse(
#'           stringr::str_detect(tableFields, "s"),
#'           "cohort_start_date",
#'           "cohort_end_date"
#'         ),
#'         alias2StartDate = ifelse(
#'           stringr::str_detect(tableFields2, "s"),
#'           "cohort_start_date",
#'           "cohort_end_date"
#'         )
#'       ) |>
#'       dplyr::filter(alias1 != alias2)
#'     
#'     tableAndDatesCombinations <-
#'       dplyr::bind_rows(
#'         tableAndDatesCombinations |>
#'           dplyr::mutate(limitToPositive = FALSE),
#'         tableAndDatesCombinations |>
#'           dplyr::mutate(
#'             limitToPositive = TRUE,
#'             outputName = paste0(outputName,
#'                                 "_P")
#'           )
#'       )
#'     
#'     targetCohort <- DatabaseConnector::renderTranslateQuerySql(
#'       connection = connection,
#'       sql = "SELECT cohort_definition_id target_cohort_definition_id,
#'                     subject_id,
#'                     cohort_start_date target_cohort_start_date,
#'                     cohort_end_date target_cohort_end_date
#'               FROM @cohort_database_schema.@cohort_table
#'               WHERE cohort_definition_id IN (@target_cohort_id)
#'               ORDER BY cohort_definition_id, subject_id, cohort_start_date;",
#'       snakeCaseToCamelCase = TRUE,
#'       cohort_database_schema = targetCohortDatabaseSchema,
#'       cohort_table = targetCohortTableName,
#'       target_cohort_id = targetCohortId
#'     ) |>
#'       dplyr::tibble()
#'     
#'     for (i in (1:nrow(tableAndDatesCombinations))) {
#'       data <- DatabaseConnector::renderTranslateQuerySql(
#'         connection = connection,
#'         sql = sqlQuery,
#'         snakeCaseToCamelCase = TRUE,
#'         alias_1 = tableAndDatesCombinations[i, ]$alias1,
#'         alias_1_start_date = tableAndDatesCombinations[i, ]$alias1StartDate,
#'         output_name = tableAndDatesCombinations[i, ]$outputName,
#'         alias_2 = tableAndDatesCombinations[i, ]$alias2,
#'         alias_2_start_date = tableAndDatesCombinations[i, ]$alias2StartDate,
#'         target_cohort_database_schema = targetCohortDatabaseSchema,
#'         target_cohort_table_name = targetCohortTableName,
#'         target_cohort_id = targetCohortId,
#'         feature_cohort_database_schema = featureCohortDatabaseSchema,
#'         feature_cohort_table_name = featureCohortTableName,
#'         feature_cohort_id = featureCohortId,
#'         limit_to_positive = tableAndDatesCombinations[i, ]$limitToPositive
#'       ) |>
#'         dplyr::tibble()
#'       
#'       suppressMessages(targetCohort <- targetCohort |>
#'                          dplyr::left_join(data))
#'     }
#'     
#'     cohortVars <-
#'       tableAndDatesCombinations$outputName |> SqlRender::snakeCaseToCamelCase() |>
#'       unique() |>
#'       sort()
#'     
#'     groupVars <-
#'       c("targetCohortDefinitionId", "featureCohortDefinitionId")
#'     
#'     for (var in cohortVars) {
#'       data <- targetCohort |>
#'         dplyr::filter(!!rlang::sym(paste0(substr(var, 1, 4), "Closest")) == 1)
#'       
#'       # Sending the minVar to summarizeCohortNumbers function
#'       output[[var]] <-
#'         summarizeNumber(
#'           data = data,
#'           number = var,
#'           groupBy = groupVars,
#'           countDistinctOccurrencesOf = "subjectId"
#'         )
#'     }
#'     
#'     # Ensure each element in output[cohortVars] is a data frame
#'     cohortDataFrames <-
#'       lapply(cohortVars, function(var)
#'         output[[var]])
#'     
#'     # Use purrr::reduce to iteratively left join these data frames
#'     summary <-
#'       purrr::reduce(cohortDataFrames, dplyr::left_join, by = groupVars)
#'     
#'     output <- c()
#'     if (returnCohortData) {
#'       output$targetCohort <- targetCohort
#'       output$summary <- summary
#'     } else {
#'       output <- summary
#'     }
#'     
#'     return(output)
#'   }
