#' Render, Translate, and Execute SQL Queries in Parallel Across Multiple CDM Sources
#'
#' This function is designed to execute SQL queries across multiple Common Data Model (CDM) sources in parallel.
#' It filters the CDM sources based on specified criteria, renders and translates SQL queries for each source,
#' and then executes them in parallel using a cluster of worker nodes.
#'
#' @param cdmSources A dataframe containing details of the CDM sources.
#' @param sequence A sequence number to filter the CDM sources, defaults to 1.
#' @param sql The SQL query string to be executed.
#' @param tempEmulationSchema Temporary schema for SQL Render emulation, obtained from global option 'sqlRenderTempEmulationSchema' if not provided.
#' @param databaseIds A vector of database IDs to filter the CDM sources, obtained from the getListOfDatabaseIds() function if not provided.
#' @param userService The user service credential key, defaults to "OHDSI_USER".
#' @param passwordService The password service credential key, defaults to "OHDSI_PASSWORD".
#' @param ... Additional parameters that may be passed to the SQL rendering and translation functions.
#' @export
renderTranslateExecuteSqlInParallel <- function(cdmSources,
                                                sequence = 1,
                                                sql,
                                                tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                                databaseIds = getListOfDatabaseIds(),
                                                userService = "OHDSI_USER",
                                                passwordService = "OHDSI_PASSWORD",
                                                ...) {

  cdmSources <-
    getCdmSource(cdmSources = cdmSources,
                 database = databaseIds,
                 sequence = sequence)

  # Convert the filtered cdmSources to a list for parallel processing
  x <- list()
  for (i in 1:nrow(cdmSources)) {
    x[[i]] <- cdmSources[i, ]
  }

  # Initialize a cluster for parallel execution
  cluster <- ParallelLogger::makeCluster(numberOfThreads = min(as.integer(trunc(
    parallel::detectCores() / 2
  )), length(x)))

  # Inner function to render, translate, and execute SQL for each CDM source
  renderTranslateExecuteSqlX <-
    function(x, sql, tempEmulationSchema, ...) {
      # Create connection details for each CDM source
      connectionDetails <- createConnectionDetails(cdmSources = x, database = x$database)
      
      connection <-
        DatabaseConnector::connect(connectionDetails = connectionDetails)

      # Render, translate, and execute SQL for the CDM source
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = sql,
        profile = FALSE,
        progressBar = FALSE,
        reportOverallTime = FALSE,
        cohort_database_schema = x$cohortDatabaseSchema,
        vocabulary_databaseS_schema = x$vocabularyDatabaseSchemaFinal,
        cdm_database_schema = x$cdmDatabaseSchema,
        ...
      )
    }

  # Apply the SQL execution function in parallel across the cluster
  ParallelLogger::clusterApply(
    cluster = cluster,
    x = x,
    fun = renderTranslateExecuteSqlX,
    sql = sql,
    ...
  )

  # Stop the cluster after execution
  ParallelLogger::stopCluster(cluster = cluster)
}
