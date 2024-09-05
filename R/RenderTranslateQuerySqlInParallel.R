#' Render SQL Queries in Parallel Across Multiple CDM Sources
#'
#' This function executes SQL queries in parallel across multiple Common Data Model (CDM) sources. It filters
#' the CDM sources based on specified criteria, renders and translates SQL queries for each source, and then
#' executes them in parallel using a cluster of worker nodes.
#'
#' @param cdmSources A dataframe containing details of the CDM sources.
#' @param sequence A sequence number to filter the CDM sources. Defaults to 1.
#' @param sql The SQL query string to be executed.
#' @param tempEmulationSchema Temporary schema for SQL Render emulation. Obtained from the global option 'sqlRenderTempEmulationSchema' if not provided.
#' @param databaseIds A vector of database IDs to filter the CDM sources. Obtained from the getListOfDatabaseIds() function if not provided.
#' @param userService The user service credential key, defaults to "OHDSI_USER".
#' @param passwordService The password service credential key, defaults to "OHDSI_PASSWORD".
#' @param ... Additional parameters that may be passed to the SQL rendering and translation functions.
#' @export
renderTranslateQuerySqlInParallel <- function(cdmSources,
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

  # Create a temporary directory for storing output files
  outputLocation <-
    file.path(tempfile(), paste0("c", sample(1:1000, 1)))
  dir.create(
    path = outputLocation,
    showWarnings = FALSE,
    recursive = TRUE
  )

  # Initialize a cluster for parallel execution
  cluster <-
    ParallelLogger::makeCluster(numberOfThreads = min(as.integer(trunc(
      parallel::detectCores() / 2
    )), length(x)))

  # Inner function to render and translate SQL for each CDM source
  renderTranslateQuerySqlX <-
    function(x,
             sql,
             tempEmulationSchema,
             outputLocation,
             ...) {
      # Create connection details for each CDM source
      connectionDetails <-
        DatabaseConnector::createConnectionDetails(
          dbms = x$dbms,
          user = keyring::key_get(userService),
          password = keyring::key_get(passwordService),
          server = x$serverFinal,
          port = x$port
        )
      connection <-
        DatabaseConnector::connect(connectionDetails = connectionDetails)

      # Render and translate SQL for the CDM source
      output <- DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = sql,
        cohort_database_schema = x$cohortDatabaseSchema,
        vocabulary_databaseS_schema = x$vocabularyDatabaseSchemaFinal,
        cdm_database_schema = x$cdmDatabaseSchema,
        snakeCaseToCamelCase = TRUE,
        ...
      ) |>
        dplyr::tibble() |>
        dplyr::tibble(
          databaseKey = x$sourceKey,
          databaseId = x$database
        )

      # Save the output to the specified location
      saveRDS(
        object = output,
        file = file.path(outputLocation, paste0(x$sourceKey, ".RDS"))
      )
    }

  # Apply the rendering function in parallel across the cluster
  ParallelLogger::clusterApply(
    cluster = cluster,
    x = x,
    fun = renderTranslateQuerySqlX,
    sql = sql,
    outputLocation = outputLocation,
    ...
  )

  # Stop the cluster after execution
  ParallelLogger::stopCluster(cluster = cluster)

  # Aggregate results from all CDM sources
  sourceKeys <- cdmSources$sourceKey
  filesToRead <-
    list.files(
      path = outputLocation,
      pattern = ".RDS",
      all.files = TRUE,
      full.names = TRUE,
      recursive = TRUE,
      ignore.case = TRUE,
      include.dirs = TRUE
    )

  if (length(filesToRead) > 0) {
    df <-
      dplyr::tibble(fileNames = filesToRead) |>
      dplyr::filter(stringr::str_detect(string = .data$fileNames, pattern = sourceKeys))

    # Read and combine all outputData data
    outputData <- c()
    for (i in (1:nrow(df))) {
      outputData[[i]] <- readRDS(df[i, ]$fileNames)
    }

    outputData <- dplyr::bind_rows(outputData)

    return(outputData)
  }
}
