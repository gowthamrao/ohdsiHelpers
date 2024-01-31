#' Execute feature extraction
#'
#' @details
#' This function executes the cohort covariate
#'
#' @param connectionDetails                   An object of type \code{connectionDetails} as created
#'                                            using the
#'                                            \code{\link[DatabaseConnector]{createConnectionDetails}}
#'                                            function in the DatabaseConnector package.
#' @param cdmDatabaseSchema                   Schema name where your patient-level data in OMOP CDM
#'                                            format resides. Note that for SQL Server, this should
#'                                            include both the database and schema name, for example
#'                                            'cdm_data.dbo'.
#' @param cohortDatabaseSchema                Schema with instantiated target cohorts.
#' @param cohortIds                           cohort ids.
#' @param cohortTable                         cohort Table Names
#' @param tempEmulationSchema                 Some database platforms like Oracle and Impala do not
#'                                            truly support temp tables. To emulate temp tables,
#'                                            provide a schema with write privileges where temp tables
#'                                            can be created.
#' @param outputFolder                        Name of local folder to place results; make sure to use
#'                                            forward slashes (/). Do not use a folder on a network
#'                                            drive since this greatly impacts performance.
#' @param covariateSettings                   FeatureExtraction covariateSettings object
#'
#' @export
executeFeatureExtraction <-
  function(connectionDetails,
           cdmDatabaseSchema,
           vocabularyDatabaseSchema = cdmDatabaseSchema,
           cohortDatabaseSchema,
           cohortIds,
           cohortTable,
           covariateSettings = FeatureExtraction::createDefaultCovariateSettings(),
           tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
           minCellCount = 5,
           minCharacterizationMean = 0.00001,
           notes = NULL,
           outputFolder) {
    if (!file.exists(outputFolder)) {
      dir.create(outputFolder, recursive = TRUE)
    }
    
    ParallelLogger::addDefaultFileLogger(file.path(outputFolder, "log.txt"))
    ParallelLogger::addDefaultErrorReportLogger(file.path(outputFolder, "errorReportR.txt"))
    on.exit(ParallelLogger::unregisterLogger("DEFAULT_FILE_LOGGER", silent = TRUE))
    on.exit(
      ParallelLogger::unregisterLogger("DEFAULT_ERRORREPORT_LOGGER", silent = TRUE),
      add = TRUE
    )
    
    connection <-
      DatabaseConnector::connect(connectionDetails = connectionDetails)
    
    ParallelLogger::logInfo(" - Beginning Feature Extraction")
    covariateData <-
      FeatureExtraction::getDbCovariateData(
        connection = connection,
        oracleTempSchema = tempEmulationSchema,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortDatabaseSchema = cohortDatabaseSchema,
        cdmVersion = 5,
        cohortTable = cohortTable,
        cohortId = cohortIds,
        covariateSettings = covariateSettings,
        aggregated = TRUE
      )
    
    FeatureExtraction::saveCovariateData(covariateData = covariateData,
                                         file = file.path(outputFolder, "covariateData"))
    
    DatabaseConnector::disconnect(connection = connection)
    
    return(covariateData)
  }


#'
#' @export
executeFeatureExtractionInParallel <-
  function(cdmSources,
           cohortIds,
           cohortTable,
           userService = "OHDSI_USER",
           passwordService = "OHDSI_PASSWORD",
           databaseIds = getListOfDatabaseIds(),
           covariateSettings = FeatureExtraction::createDefaultCovariateSettings(),
           sequence = 1,
           maxCores = parallel::detectCores() /
             3,
           databaseId = NULL,
           tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
           outputFolder) {
    cdmSources <- cdmSources |>
      dplyr::filter(.data$database %in% c(databaseIds)) |>
      dplyr::filter(.data$sequence == !!sequence)
    
    x <- list()
    for (i in 1:nrow(cdmSources)) {
      x[[i]] <- cdmSources[i, ]
    }
    
    # use Parallel Logger to run in parallel
    cluster <-
      ParallelLogger::makeCluster(numberOfThreads = min(as.integer(trunc(
        parallel::detectCores() /
          2
      )),
      nrow(cdmSources)),
      maxCores)
    
    ## file logger
    loggerName <-
      paste0(
        "CFe_",
        stringr::str_replace_all(
          string = Sys.time(),
          pattern = ":|-|EDT| ",
          replacement = ""
        )
      )
    
    ParallelLogger::addDefaultFileLogger(fileName = file.path(outputFolder, paste0(loggerName, ".txt")))
    
    executeFeatureExtractionX <-
      function(x,
               cohortIds,
               cohortTable,
               userService,
               passwordService,
               tempEmulationSchema,
               covariateSettings,
               outputFolder) {
        connectionDetails <- DatabaseConnector::createConnectionDetails(
          dbms = x$dbms,
          user = keyring::key_get(userService),
          password = keyring::key_get(passwordService),
          server = x$serverFinal,
          port = x$port
        )
        
        outputFolder <-
          file.path(outputFolder, x$sourceKey)
        
        dir.create(path = outputFolder,
                   showWarnings = FALSE,
                   recursive = TRUE)
        
        executeFeatureExtraction(
          connectionDetails = connectionDetails,
          cdmDatabaseSchema = x$cdmDatabaseSchemaFinal,
          cohortDatabaseSchema = x$cohortDatabaseSchemaFinal,
          cohortIds = cohortIds,
          cohortTable = cohortTable,
          covariateSettings = covariateSettings,
          outputFolder = outputFolder
        )
      }
    
    ParallelLogger::clusterApply(
      cluster = cluster,
      x = x,
      fun = executeFeatureExtractionX,
      cohortIds = cohortIds,
      cohortTable = cohortTable,
      userService = userService,
      passwordService = passwordService,
      covariateSettings = covariateSettings,
      tempEmulationSchema = tempEmulationSchema,
      outputFolder = outputFolder
    )
    
    ParallelLogger::stopCluster(cluster = cluster)
  }
