#' @export
renderTranslateExecuteSqlInParallel <-
  function(cdmSources,
           sequence = 1,
           sql,
           tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
           databaseIds = getListOfDatabaseIds(),
           userService = "OHDSI_USER",
           passwordService = "OHDSI_PASSWORD",
           ...) {
    cdmSources <- cdmSources |>
      dplyr::filter(.data$database %in% c(databaseIds)) |>
      dplyr::filter(.data$sequence == !!sequence)
    
    x <- list()
    for (i in 1:nrow(cdmSources)) {
      x[[i]] <- cdmSources[i,]
    }
    
    # use Parallel Logger to run in parallel
    cluster <-
      ParallelLogger::makeCluster(numberOfThreads = min(as.integer(trunc(
        parallel::detectCores() /
          2
      )),
      length(x)))
    
    renderTranslateExecuteSqlX <- function(x,
                                           sql,
                                           tempEmulationSchema,
                                           ...) {
      connectionDetails <- DatabaseConnector::createConnectionDetails(
        dbms = x$dbms,
        user = keyring::key_get(userService),
        password = keyring::key_get(passwordService),
        server = x$serverFinal,
        port = x$port
      )
      connection = DatabaseConnector::connect(connectionDetails = connectionDetails)
      
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = sql,
        profile = FALSE,
        progressBar = FALSE,
        reportOverallTime = FALSE,
        cohort_database_schema = x$cohortDatabaseSchemaFinal,
        vocabulary_databaseS_schema = x$vocabularyDatabaseSchemaFinal,
        cdm_database_schema = x$cdmDatabaseSchemaFinal,
        ...
      )
      
    }
    ParallelLogger::clusterApply(
      cluster = cluster,
      x = x,
      fun = renderTranslateExecuteSqlX,
      sql = sql,
      ...
    )
    
    ParallelLogger::stopCluster(cluster = cluster)
    
  }