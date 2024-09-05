#' # Copyright 2023 Observational Health Data Sciences and Informatics
#' #
#' # This file is part of OhdsiHelpers
#' #
#' # Licensed under the Apache License, Version 2.0 (the "License");
#' # you may not use this file except in compliance with the License.
#' # You may obtain a copy of the License at
#' #
#' #     http://www.apache.org/licenses/LICENSE-2.0
#' #
#' # Unless required by applicable law or agreed to in writing, software
#' # distributed under the License is distributed on an "AS IS" BASIS,
#' # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#' # See the License for the specific language governing permissions and
#' # limitations under the License.
#' #
#'
#'
#' #' @export
#' executeTimeDistributionDiagnostics <-
#'   function(connectionDetails = NULL,
#'            connection = NULL,
#'            cohortIds,
#'            cohortDatabaseSchema,
#'            cdmDatabaseSchema,
#'            cohortTableName) {
#'     sql <- "
#'   DROP TABLE IF EXISTS #date_difference;
#' 	SELECT c.cohort_definition_id,
#' 		c.subject_id,
#' 		c.cohort_end_date - c.cohort_start_date AS in_cohort,
#' 		c.cohort_start_date - op.observation_period_start_date AS bf_cohort,
#' 		op.observation_period_end_date - c.cohort_end_date AS af_cohort
#' 	INTO #date_difference
#' 	FROM @cohort_database_schema.@cohort_table_name c
#' 	INNER JOIN @cdm_database_schema.observation_period op ON c.subject_id = op.person_id
#' 		AND c.cohort_start_date >= op.observation_period_start_date
#' 		AND c.cohort_end_date <= op.observation_period_end_date
#' 	WHERE cohort_definition_id IN (@cohort_ids);
#'
#' DROP TABLE IF EXISTS #summary_stats;
#' SELECT cohort_definition_id,
#' 	AVG(in_cohort) AS in_cohort_mean,
#' 	MIN(in_cohort) AS in_cohort_min,
#' 	MAX(in_cohort) AS in_cohort_max,
#' 	AVG(bf_cohort) AS bf_cohort_mean,
#' 	MIN(bf_cohort) AS bf_cohort_min,
#' 	MAX(bf_cohort) AS bf_cohort_max,
#' 	AVG(af_cohort) AS af_cohort_mean,
#' 	MIN(af_cohort) AS af_cohort_min,
#' 	MAX(af_cohort) AS af_cohort_max
#' INTO #summary_stats
#' FROM #date_difference
#' GROUP BY cohort_definition_id;
#'
#' DROP TABLE IF EXISTS #distribution_before;
#' SELECT cohort_definition_id,
#' 	PERCENTILE_CONT(0.01) WITHIN
#' GROUP (
#' 		ORDER BY in_cohort
#' 		) AS in_cohort_p1,
#' 	PERCENTILE_CONT(0.10) WITHIN
#' GROUP (
#' 		ORDER BY in_cohort
#' 		) AS in_cohort_p10,
#' 	PERCENTILE_CONT(0.25) WITHIN
#' GROUP (
#' 		ORDER BY in_cohort
#' 		) AS in_cohort_p25,
#' 	PERCENTILE_CONT(0.50) WITHIN
#' GROUP (
#' 		ORDER BY in_cohort
#' 		) AS in_cohort_p50,
#' 	PERCENTILE_CONT(0.75) WITHIN
#' GROUP (
#' 		ORDER BY in_cohort
#' 		) AS in_cohort_p75,
#' 	PERCENTILE_CONT(0.90) WITHIN
#' GROUP (
#' 		ORDER BY in_cohort
#' 		) AS in_cohort_p90,
#' 	PERCENTILE_CONT(0.95) WITHIN
#' GROUP (
#' 		ORDER BY in_cohort
#' 		) AS in_cohort_p95,
#' 	PERCENTILE_CONT(0.99) WITHIN
#' GROUP (
#' 		ORDER BY in_cohort
#' 		) AS in_cohort_p99
#' INTO #distribution_before
#' FROM #date_difference
#' GROUP BY cohort_definition_id;
#'
#' DROP TABLE #distribution_during;
#' SELECT cohort_definition_id,
#' 	PERCENTILE_CONT(0.01) WITHIN
#' GROUP (
#' 		ORDER BY bf_cohort
#' 		) AS bf_cohort_p1,
#' 	PERCENTILE_CONT(0.10) WITHIN
#' GROUP (
#' 		ORDER BY bf_cohort
#' 		) AS bf_cohort_p10,
#' 	PERCENTILE_CONT(0.25) WITHIN
#' GROUP (
#' 		ORDER BY bf_cohort
#' 		) AS bf_cohort_p25,
#' 	PERCENTILE_CONT(0.50) WITHIN
#' GROUP (
#' 		ORDER BY bf_cohort
#' 		) AS bf_cohort_p50,
#' 	PERCENTILE_CONT(0.75) WITHIN
#' GROUP (
#' 		ORDER BY bf_cohort
#' 		) AS bf_cohort_p75,
#' 	PERCENTILE_CONT(0.90) WITHIN
#' GROUP (
#' 		ORDER BY bf_cohort
#' 		) AS bf_cohort_p90,
#' 	PERCENTILE_CONT(0.95) WITHIN
#' GROUP (
#' 		ORDER BY bf_cohort
#' 		) AS bf_cohort_p95,
#' 	PERCENTILE_CONT(0.99) WITHIN
#' GROUP (
#' 		ORDER BY bf_cohort
#' 		) AS bf_cohort_p99
#' INTO #distribution_during
#' FROM #date_difference AS dd
#' GROUP BY cohort_definition_id;
#'
#' DROP TABLE IF EXISTS #distribution_after;
#' SELECT cohort_definition_id,
#' 	PERCENTILE_CONT(0.01) WITHIN
#' GROUP (
#' 		ORDER BY af_cohort
#' 		) AS af_cohort_p1,
#' 	PERCENTILE_CONT(0.10) WITHIN
#' GROUP (
#' 		ORDER BY af_cohort
#' 		) AS af_cohort_p10,
#' 	PERCENTILE_CONT(0.25) WITHIN
#' GROUP (
#' 		ORDER BY af_cohort
#' 		) AS af_cohort_p25,
#' 	PERCENTILE_CONT(0.50) WITHIN
#' GROUP (
#' 		ORDER BY af_cohort
#' 		) AS af_cohort_p50,
#' 	PERCENTILE_CONT(0.75) WITHIN
#' GROUP (
#' 		ORDER BY af_cohort
#' 		) AS af_cohort_p75,
#' 	PERCENTILE_CONT(0.90) WITHIN
#' GROUP (
#' 		ORDER BY af_cohort
#' 		) AS af_cohort_p90,
#' 	PERCENTILE_CONT(0.95) WITHIN
#' GROUP (
#' 		ORDER BY af_cohort
#' 		) AS af_cohort_p95,
#' 	PERCENTILE_CONT(0.99) WITHIN
#' GROUP (
#' 		ORDER BY af_cohort
#' 		) AS af_cohort_p99
#' INTO #distribution_after
#' FROM #date_difference
#' GROUP BY cohort_definition_id;
#'
#' DROP TABLE IF EXISTS #final_results;
#' SELECT a.cohort_definition_id,
#' 	in_cohort_mean,
#' 	in_cohort_min,
#' 	in_cohort_max,
#' 	af_cohort_mean,
#' 	af_cohort_min,
#' 	af_cohort_max,
#' 	bf_cohort_mean,
#' 	bf_cohort_min,
#' 	bf_cohort_max,
#' 	in_cohort_p1,
#' 	in_cohort_p10,
#' 	in_cohort_p25,
#' 	in_cohort_p50,
#' 	in_cohort_p75,
#' 	in_cohort_p90,
#' 	in_cohort_p95,
#' 	in_cohort_p99,
#' 	bf_cohort_p1,
#' 	bf_cohort_p10,
#' 	bf_cohort_p25,
#' 	bf_cohort_p50,
#' 	bf_cohort_p75,
#' 	bf_cohort_p90,
#' 	bf_cohort_p95,
#' 	bf_cohort_p99,
#' 	af_cohort_p1,
#' 	af_cohort_p10,
#' 	af_cohort_p25,
#' 	af_cohort_p50,
#' 	af_cohort_p75,
#' 	af_cohort_p90,
#' 	af_cohort_p95,
#' 	af_cohort_p99
#' INTO #final_results
#' FROM #summary_stats a
#' LEFT JOIN #distribution_before b ON a.cohort_definition_id = b.cohort_definition_id
#' LEFT JOIN #distribution_during c ON a.cohort_definition_id = c.cohort_definition_id
#' LEFT JOIN #distribution_after d ON a.cohort_definition_id = d.cohort_definition_id
#' ORDER BY a.cohort_definition_id;
#'
#' DROP TABLE IF EXISTS #date_difference;
#' DROP TABLE IF EXISTS #summary_stats;
#' DROP TABLE IF EXISTS #distribution_before;
#' DROP TABLE IF EXISTS #distribution_during;
#' DROP TABLE IF EXISTS #distribution_after;
#'
#' "
#'     sqlRendered <- SqlRender::render(
#'       sql = sql,
#'       cohort_database_schema = cohortDatabaseSchema,
#'       cohort_table_name = cohortTableName,
#'       cdm_database_schema = cdmDatabaseSchema,
#'       cohort_ids = cohortIds
#'     )
#'
#'     # Set up connection to server ----------------------------------------------------
#'     if (is.null(connection)) {
#'       if (!is.null(connectionDetails)) {
#'         connection <- DatabaseConnector::connect(connectionDetails)
#'         on.exit(DatabaseConnector::disconnect(connection))
#'       } else {
#'         stop("No connection or connectionDetails provided.")
#'       }
#'     }
#'
#'     browser()
#'     DatabaseConnector::executeSql(connection = connection,
#'                                   sql = sqlRendered)
#'
#'     result <- DatabaseConnector::querySql(
#'       connection = connection,
#'       sql = "select * FROM #final_result;",
#'       snakeCaseToCamelCase = TRUE
#'     )
#'
#'     DatabaseConnector::executeSql(connection = connection,
#'                                   sql = "DROP TABLE IF EXISTS #final_result;")
#'
#'     return(result)
#'
#'   }
#'
#'
#' #' @export
#' executeTimeDistributionDiagnosticsInParallel <-
#'   function(cdmSources,
#'            outputFolder,
#'            cohortTableNames = NULL,
#'            userService = "OHDSI_USER",
#'            passwordService = "OHDSI_PASSWORD",
#'            databaseIds = getListOfDatabaseIds(),
#'            sequence = 1,
#'            cohortIds = NULL) {
#'     cdmSources <- cdmSources |>
#'       dplyr::filter(database %in% c(databaseIds)) |>
#'       dplyr::filter(sequence == !!sequence)
#'
#'     x <- list()
#'     for (i in 1:nrow(cdmSources)) {
#'       x[[i]] <- cdmSources[i,]
#'     }
#'
#'     # use Parallel Logger to run in parallel
#'     cluster <-
#'       ParallelLogger::makeCluster(numberOfThreads = min(as.integer(trunc(
#'         parallel::detectCores() /
#'           2
#'       )),
#'       length(x)))
#'
#'     ## file logger
#'     loggerName <-
#'       paste0(
#'         "TD_",
#'         stringr::str_replace_all(
#'           string = Sys.time(),
#'           pattern = ":|-|EDT| ",
#'           replacement = ''
#'         )
#'       )
#'
#'     ParallelLogger::addDefaultFileLogger(fileName = file.path(outputFolder, paste0(loggerName, ".txt")))
#'
#'
#'     executeTimeDistributionDiagnosticsX <- function(x,
#'                                                     cohortIds,
#'                                                     outputFolder,
#'                                                     cohortTableName) {
#'       connectionDetails <- DatabaseConnector::createConnectionDetails(
#'         dbms = x$dbms,
#'         user = keyring::key_get(userService),
#'         password = keyring::key_get(passwordService),
#'         server = x$serverFinal,
#'         port = x$port
#'       )
#'       outputFolder <-
#'         file.path(outputFolder, x$sourceKey)
#'
#'       executeTimeDistributionDiagnostics(
#'         connectionDetails = connectionDetails,
#'         cohortIds = cohortIds,
#'         cohortTableName = cohortTableName,
#'         cdmDatabaseSchema = x$cdmDatabaseSchema,
#'         cohortDatabaseSchema = x$cohortDatabaseSchema
#'       )
#'     }
#'
#'     ParallelLogger::clusterApply(
#'       cluster = cluster,
#'       x = x,
#'       cohortIds = cohortIds,
#'       outputFolder = outputFolder,
#'       cohortTableName = cohortTableNames$cohortTable,
#'       fun = executeTimeDistributionDiagnosticsX,
#'       stopOnError = FALSE
#'     )
#'
#'     ParallelLogger::stopCluster(cluster = cluster)
#'
#'     ParallelLogger::clearLoggers()
#'   }
