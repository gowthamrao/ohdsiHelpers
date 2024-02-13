#' @export
renderQuerySqlSqlInParallel <-
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
      x[[i]] <- cdmSources[i, ]
    }
    
    outputLocation <- tempfile()
    dir.create(path = outputLocation,
               showWarnings = FALSE,
               recursive = TRUE)
    
    # use Parallel Logger to run in parallel
    cluster <-
      ParallelLogger::makeCluster(numberOfThreads = min(as.integer(trunc(
        parallel::detectCores() /
          2
      )),
      length(x)))
    
    renderTranslateQuerySqlX <- function(x,
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
      
      output <- DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = sql,
        cohort_database_schema = x$cohortDatabaseSchemaFinal,
        vocabulary_databaseS_schema = x$vocabularyDatabaseSchemaFinal,
        cdm_database_schema = x$cdmDatabaseSchemaFinal,
        ...
      ) |>
        dplyr::tibble() |>
        dplyr::tibble(databaseKey = x$sourceKey,
                      databaseId = x$database)
      
      saveRDS(object = output,
              file = file.path(outputLocation, paste0(x$sourceKey, ".RDS")))
      
    }
    ParallelLogger::clusterApply(
      cluster = cluster,
      x = x,
      fun = renderTranslateQuerySqlX,
      sql = sql,
      ...
    )
    
    ParallelLogger::stopCluster(cluster = cluster)
    
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
    
    browser()
    
    outputData <- c()
    for (i in (1:length(filesToRead))) {
      outputData[[i]] <- readRDS(filesToRead[[i]])
    }
    
    outputData <- dplyr::bind_rows(outputData)
    
    return(outputData)
  }