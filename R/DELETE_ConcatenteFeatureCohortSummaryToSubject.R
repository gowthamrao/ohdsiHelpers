#' #' @export
#' concatenteFeatureCohortSummaryToSubjectInParallel <-
#'   function(cdmSources,
#'            userService = "OHDSI_USER",
#'            passwordService = "OHDSI_PASSWORD",
#'            tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
#'            databaseIds = getListOfDatabaseIds(),
#'            sequence = 1,
#'            cohortTableName,
#'            subjectTableName,
#'            featureCohortIds,
#'            subjectStartDate,
#'            subjectEndDate,
#'            prefix = NULL) {
#'     
#'     cdmSources <-
#'       getCdmSource(cdmSources = cdmSources,
#'                    database = databaseIds,
#'                    sequence = sequence)
#' 
#'     x <- list()
#'     for (i in 1:nrow(cdmSources)) {
#'       x[[i]] <- cdmSources[i, ]
#'     }
#' 
#'     # use Parallel Logger to run in parallel
#'     cluster <-
#'       ParallelLogger::makeCluster(numberOfThreads = min(
#'         as.integer(trunc(
#'           parallel::detectCores() /
#'             2
#'         )),
#'         length(x)
#'       ))
#' 
#'     concatenteFeatureCohortSummaryToSubjectX <- function(x,
#'                                                          featureCohortTableName,
#'                                                          featureCohortIds,
#'                                                          subjectTableName,
#'                                                          subjectStartDate,
#'                                                          subjectEndDate,
#'                                                          prefix) {
#'       connectionDetails <- DatabaseConnector::createConnectionDetails(
#'         dbms = x$dbms,
#'         user = keyring::key_get(userService),
#'         password = keyring::key_get(passwordService),
#'         server = x$serverFinal,
#'         port = x$port
#'       )
#'       connection <-
#'         DatabaseConnector::connect(connectionDetails = connectionDetails)
#' 
#'       for (j in (1:length(featureCohortIds))) {
#'         featureCohortId <- featureCohortIds[[j]]
#'         concatenteFeatureCohortSummaryToSubject(
#'           connection = connection,
#'           featureCohortTableName = featureCohortTableName,
#'           featureCohortDatabaseSchema = x$cohortDatabaseSchemaFinal,
#'           featureCohortIsTemp = FALSE,
#'           featureCohortId = featureCohortId,
#'           subjectTableName = subjectTableName,
#'           subjectTableDatabaseSchema = x$cohortDatabaseSchemaFinal,
#'           subjectTableIsTemp = FALSE,
#'           subjectStartDate = subjectStartDate,
#'           subjectEndDate = subjectEndDate,
#'           prefix = prefix
#'         )
#'       }
#'     }
#' 
#'     ParallelLogger::clusterApply(
#'       cluster = cluster,
#'       x = x,
#'       fun = concatenteFeatureCohortSummaryToSubjectX,
#'       featureCohortTableName = cohortTableName,
#'       featureCohortIds = featureCohortIds,
#'       subjectTableName = subjectTableName,
#'       subjectStartDate = subjectStartDate,
#'       subjectEndDate = subjectEndDate,
#'       prefix = prefix,
#'       stopOnError = FALSE
#'     )
#' 
#'     ParallelLogger::stopCluster(cluster = cluster)
#'   }
#' 
#' 
#' #' Concatenate Feature Cohort Summary to Subject Table
#' #'
#' #' This function performs a left join on a subject table with a feature cohort table
#' #' and adds new columns to the subject table based on the feature cohorts.
#' #' The subject table must have one row per subject and be located in a remote database,
#' #' and must contain a 'subject_id' column at the minimum.
#' #'
#' #' If 'subjectStartDate' and 'subjectEndDate' are provided, additional columns are added
#' #' to the subject table to reflect these date limits; otherwise, only the occurrence dates
#' #' of the feature cohort are returned without any start or end date limits.
#' #'
#' #' @param connection Database connection object.
#' #' @param subjectTableName Name of the subject table.
#' #' @param subjectTableDatabaseSchema (Optional) Database schema that contains the wide subject table.
#' #' @param subjectTableIsTemp Boolean flag indicating if the subject table is temporary.
#' #' @param subjectStartDate (Optional) Date field in the subject table to left-censor the occurrence check.
#' #' @param subjectEndDate (Optional) Date field in the subject table to right-censor the occurrence check.
#' #' @param featureCohortTableName Name of the feature cohort table.
#' #' @param featureCohortDatabaseSchema Database schema containing the feature cohort table.
#' #' @param featureCohortIsTemp Boolean flag indicating if the feature cohort table is temporary.
#' #' @param featureCohortId Identifier for the feature cohort.
#' #' @param tempEmulationSchema (Optional) Schema for temporary emulation.
#' #' @param prefix (Optional) Prefix for the new columns added to the subject table. If NULL, then prefix will be the feature cohort id such as c1234
#' #'
#' #' @return A tibble listing the new columns added to the subject table.
#' #' @export
#' concatenteFeatureCohortSummaryToSubject <-
#'   function(connection,
#'            subjectTableName,
#'            # subject table is not a OHDSI cohort table. it is wide table with one row per subjectId
#'            subjectTableDatabaseSchema = NULL,
#'            # the database schema that has the wide subjectTable
#'            subjectTableIsTemp = FALSE,
#'            subjectStartDate = NULL,
#'            # a date field in the subjectTable to left censor the check of occurrence of a feature
#'            subjectEndDate = NULL,
#'            # a date field in the subjectTable to right censor the check of occurrence of a feature
#'            featureCohortTableName,
#'            featureCohortDatabaseSchema,
#'            featureCohortIsTemp = FALSE,
#'            featureCohortId,
#'            tempEmulationSchema = NULL,
#'            prefix = NULL) {
#'     if (is.null(prefix)) {
#'       prefix <- paste0("C", featureCohortId)
#'     }
#' 
#'     limitToSubjectDates <- FALSE
#'     if (any(!is.null(subjectStartDate), !is.null(subjectEndDate))) {
#'       limitToSubjectDates <- TRUE
#'     }
#' 
#'     useLowerLimitDate <- FALSE
#'     if (!is.null(subjectStartDate)) {
#'       useLowerLimitDate <- TRUE
#'     }
#' 
#'     useUpperLimitDate <- FALSE
#'     if (!is.null(subjectEndDate)) {
#'       useUpperLimitDate <- TRUE
#'     }
#' 
#'     cohortRelationshipSql <-
#'       SqlRender::readSql(
#'         sourceFile = system.file(
#'           "sql",
#'           "sql_server",
#'           "ConcatenateFeatureCohort.sql",
#'           package = utils::packageName()
#'         )
#'       )
#' 
#'     checkIfOneRowPerSubject <-
#'       DatabaseConnector::renderTranslateQuerySql(
#'         connection = connection,
#'         sql = "SELECT subject_id from @cohort_database_schema.@subject_table_name GROUP BY subject_id HAVING COUNT(*) > 1;",
#'         cohort_database_schema = subjectTableDatabaseSchema,
#'         subject_table_name = subjectTableName,
#'         snakeCaseToCamelCase = TRUE,
#'         tempEmulationSchema = tempEmulationSchema
#'       )
#'     if (nrow(checkIfOneRowPerSubject) > 0) {
#'       stop(
#'         paste0(
#'           "Check subject table ",
#'           subjectTableDatabaseSchema,
#'           ".",
#'           subjectTableName,
#'           " ; Has more than one record per subjectId."
#'         )
#'       )
#'     }
#' 
#'     DatabaseConnector::renderTranslateExecuteSql(
#'       connection = connection,
#'       sql = sql,
#'       cohort_database_schema = featureCohortDatabaseSchema,
#'       cohort_table_name = featureCohortTableName,
#'       feature_definition_id = featureCohortId,
#'       feature_cohort_table_is_temp = featureCohortIsTemp,
#'       subject_table_is_temp = subjectTableIsTemp,
#'       subject_table_database_schema = subjectTableDatabaseSchema,
#'       subject_table_name = subjectTableName,
#'       prefix = prefix,
#'       limit_to_subject_dates = limitToSubjectDates,
#'       use_lower_limit_date = useLowerLimitDate,
#'       lower_limit_date = subjectStartDate,
#'       use_upper_limit_date = useUpperLimitDate,
#'       upper_limit_date = subjectEndDate
#'     )
#' 
#' 
#'     columnsCreated <- dplyr::tibble(
#'       columnsCreated = c(
#'         "@prefix_fs",
#'         "@prefix_fe",
#'         "@prefix_fd",
#'         "@prefix_ls",
#'         "@prefix_le",
#'         "@prefix_ld",
#'         "@prefix_sn",
#'         "@prefix_ev",
#'         "@prefix_t",
#'         "@prefix_fs_btn",
#'         "@prefix_fe_btn",
#'         "@prefix_fd_btn",
#'         "@prefix_ls_btn",
#'         "@prefix_le_btn",
#'         "@prefix_ld_btn",
#'         "@prefix_sn_btn",
#'         "@prefix_ev_btn",
#'         "@prefix_t_btn",
#'         "@prefix_fs_on_ll",
#'         "@prefix_fe_on_ll",
#'         "@prefix_fd_on_ll",
#'         "@prefix_ls_on_ll",
#'         "@prefix_le_on_ll",
#'         "@prefix_ld_on_ll",
#'         "@prefix_sn_on_ll",
#'         "@prefix_ev_on_ll",
#'         "@prefix_t_on_ll",
#'         "@prefix_fs_aft_ll",
#'         "@prefix_fe_aft_ll",
#'         "@prefix_fd_aft_ll",
#'         "@prefix_ls_aft_ll",
#'         "@prefix_le_aft_ll",
#'         "@prefix_ld_aft_ll",
#'         "@prefix_sn_aft_ll",
#'         "@prefix_ev_aft_ll",
#'         "@prefix_t_aft_ll",
#'         "@prefix_fs_bf_ll",
#'         "@prefix_fe_bf_ll",
#'         "@prefix_fd_bf_ll",
#'         "@prefix_ls_bf_ll",
#'         "@prefix_le_bf_ll",
#'         "@prefix_ld_bf_ll",
#'         "@prefix_sn_bf_ll",
#'         "@prefix_ev_bf_ll",
#'         "@prefix_t_bf_ll",
#'         "@prefix_fs_on_ul",
#'         "@prefix_fe_on_ul",
#'         "@prefix_fd_on_ul",
#'         "@prefix_ls_on_ul",
#'         "@prefix_le_on_ul",
#'         "@prefix_ld_on_ul",
#'         "@prefix_sn_on_ul",
#'         "@prefix_ev_on_ul",
#'         "@prefix_t_on_ul",
#'         "@prefix_fs_bf_ul",
#'         "@prefix_fe_bf_ul",
#'         "@prefix_fd_bf_ul",
#'         "@prefix_ls_bf_ul",
#'         "@prefix_le_bf_ul",
#'         "@prefix_ld_bf_ul",
#'         "@prefix_sn_bf_ul",
#'         "@prefix_ev_bf_ul",
#'         "@prefix_t_bf_ul",
#'         "@prefix_fs_aft_ul",
#'         "@prefix_fe_aft_ul",
#'         "@prefix_fd_aft_ul",
#'         "@prefix_ls_aft_ul",
#'         "@prefix_le_aft_ul",
#'         "@prefix_ld_aft_ul",
#'         "@prefix_sn_aft_ul",
#'         "@prefix_ev_aft_ul",
#'         "@prefix_t_aft_ul"
#'       )
#'     )
#' 
#'     result <- c()
#'     for (i in (1:nrow(columnsCreated))) {
#'       result[[i]] <-
#'         parseColumnText(columnsCreated[i, ]$columnsCreated) |>
#'         dplyr::mutate(columnsCreated = columnsCreated[i, ]$columnsCreated)
#'     }
#' 
#'     result <- result |>
#'       dplyr::bind_rows() |>
#'       dplyr::mutate(columnsCreated = stringr::str_replace(
#'         string = columnsCreated,
#'         pattern = "@prefix",
#'         replacement = prefix
#'       )) |>
#'       dplyr::relocate(columnsCreated)
#' 
#'     return(result)
#'   }
#' 
#' 
#' 
#' 
#' #' Parse Column Text
#' #'
#' #' This function takes a string representing a column name pattern and returns
#' #' a tibble with both concise and detailed explanations for each pattern component.
#' #' The pattern components are identified by splitting the input text at underscores.
#' #'
#' #' @param text A string representing the column name pattern.
#' #'
#' #' @return A tibble with two columns: 'explanation' and 'detailed'.
#' #'         Each column contains concatenated explanations for the pattern components.
#' #'
#' #' @examples
#' #' parseColumnText("prefix_fd_btn")
#' #' # Returns a tibble with explanations for '_fd' and '_btn' patterns.
#' parseColumnText <- function(text) {
#'   # Define the column explanations in a tibble
#'   columnExplanations <- dplyr::tibble(
#'     columnPattern = c(
#'       "_fs",
#'       "_fe",
#'       "_fd",
#'       "_ls",
#'       "_le",
#'       "_ld",
#'       "_sn",
#'       "_ev",
#'       "_t",
#'       "_btn",
#'       "_ll",
#'       "_ul",
#'       "_on",
#'       "_bf",
#'       "_af"
#'     ),
#'     explanation = c(
#'       "first start",
#'       "first end",
#'       "first days",
#'       "last start",
#'       "last end",
#'       "last days",
#'       "days between extremes",
#'       "number of events",
#'       "occurs",
#'       "between",
#'       "to lower limit date",
#'       "to upper limit date",
#'       "matches",
#'       "is before",
#'       "if after"
#'     ),
#'     detailed = c(
#'       "First Start",
#'       "First End",
#'       "Days between First Start and First End",
#'       "Last Start",
#'       "Last End",
#'       "Days between Last Start and Last End",
#'       "Days between First Start and Last End",
#'       "Number of Events",
#'       "Occurs (Indicates whether the feature occurs, 0 for no, 1 for yes)",
#'       "Between specified lower and upper limit date fields in subject table, both inclusive",
#'       "In relation to specified lower limit date fields only in subject table. Upper limit date ignored",
#'       "In relation to specified upper limit date fields only in subject table. Lower limit date ignored",
#'       "Feature cohort date matches specified date field in subject table",
#'       "Feature cohort date is before specified date field in subject table",
#'       "Feature cohort date is after specified date field in subject table"
#'     )
#'   )
#' 
#'   # Split the input text into its components
#'   patterns <- strsplit(text, "_")[[1]]
#'   patterns <- patterns[-1] # Remove the first element (prefix)
#' 
#'   # Find matching concise explanations for each pattern
#'   explanations <- sapply(patterns, function(pattern) {
#'     explanation <-
#'       columnExplanations$explanation[which(columnExplanations$columnPattern == paste0("_", pattern))]
#'     if (length(explanation) == 0) {
#'       return(NA)
#'     }
#'     return(explanation)
#'   })
#' 
#'   # Find matching detailed explanations for each pattern
#'   detailed <- sapply(patterns, function(pattern) {
#'     detailed <-
#'       columnExplanations$detailed[which(columnExplanations$columnPattern == paste0("_", pattern))]
#'     if (length(detailed) == 0) {
#'       return(NA)
#'     }
#'     return(detailed)
#'   })
#' 
#'   # Concatenate the explanations into a tibble
#'   result <- dplyr::tibble(
#'     explanation = paste(explanations, collapse = " - "),
#'     detailed = paste(detailed, collapse = " - ")
#'   )
#'   return(result)
#' }
