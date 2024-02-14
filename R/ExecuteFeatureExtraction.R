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
           connection,
           cdmDatabaseSchema,
           vocabularyDatabaseSchema = cdmDatabaseSchema,
           cohortDatabaseSchema,
           cohortIds,
           cohortTable,
           runCohortBasedTemporalCharacterization = TRUE,
           covariateCohortDatabaseSchema = NULL,
           covariateCohortIds = NULL,
           covariateCohortTable = cohortTable,
           covariateCohortDefinitionSet = NULL,
           cohortCovariateAnalysisId = 150,
           covariateSettings = FeatureExtraction::createDefaultCovariateSettings(),
           tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
           outputFolder) {
    if (!file.exists(outputFolder)) {
      dir.create(outputFolder, recursive = TRUE)
    }
    
    if (runCohortBasedTemporalCharacterization) {
      if (covariateSettings$temporal) {
        if (is.null(covariateCohortDefinitionSet)) {
          stop(
            "covariateCohortDefinitionSet is NULL. cannot run cohort temporal characterization"
          )
        }
        
        if (!is.null(covariateCohortIds)) {
          covariateCohortDefinitionSet <- covariateCohortDefinitionSet |>
            dplyr::filter(cohortId %in% covariateCohortIds)
        }
        
        if (is.null(covariateCohortTable)) {
          stop("covariateCohortTable is NULL. cannot run cohort temporal characterization")
        }
        if (is.null(covariateCohortDatabaseSchema)) {
          stop(
            "covariateCohortDatabaseSchema is NULL. cannot run cohort temporal characterization"
          )
        }
        if (is.null(cohortCovariateAnalysisId)) {
          stop(
            "cohortCovariateAnalysisId is NULL. cannot run cohort temporal characterization"
          )
        }
        
        temporalStartDays <- covariateSettings$temporalStartDays
        temporalEndDays <- covariateSettings$temporalEndDays
        
        if (is.null(covariateCohortIds)) {
          covariateCohortIds <-
            covariateCohortDefinitionSet$cohortId |> unique() |> sort()
        }
        
        cohortBasedTemporalCovariateSettings <-
          FeatureExtraction::createCohortBasedTemporalCovariateSettings(
            analysisId = cohortCovariateAnalysisId,
            covariateCohortDatabaseSchema = covariateCohortDatabaseSchema,
            covariateCohortTable = covariateCohortTable,
            covariateCohorts = covariateCohortDefinitionSet |>
              dplyr::filter(.data$cohortId %in% c(covariateCohortIds)),
            valueType = "binary",
            temporalStartDays = temporalStartDays,
            temporalEndDays = temporalEndDays
          )
        
        covariateSettings <- list(covariateSettings,
                                  cohortBasedTemporalCovariateSettings)
        
      } else {
        stop(
          "Cant run cohort based temporal characterization because covariate setting is not temporal"
        )
      }
    }
    
    ParallelLogger::addDefaultFileLogger(file.path(outputFolder, "log.txt"))
    ParallelLogger::addDefaultErrorReportLogger(file.path(outputFolder, "errorReportR.txt"))
    on.exit(ParallelLogger::unregisterLogger("DEFAULT_FILE_LOGGER", silent = TRUE))
    on.exit(
      ParallelLogger::unregisterLogger("DEFAULT_ERRORREPORT_LOGGER", silent = TRUE),
      add = TRUE
    )
    
    if (is.null(connection)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    }
    
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
           covariateSettings = CohortDiagnostics::getDefaultCovariateSettings(),
           runCohortBasedTemporalCharacterization = TRUE,
           covariateCohortIds = NULL,
           covariateCohortTable = cohortTable,
           covariateCohortDefinitionSet = NULL,
           cohortCovariateAnalysisId = 150,
           sequence = 1,
           maxCores = parallel::detectCores() /
             3,
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
               runCohortBasedTemporalCharacterization,
               covariateCohortIds,
               covariateCohortTable,
               covariateCohortDefinitionSet,
               cohortCovariateAnalysisId,
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
          outputFolder = outputFolder,
          runCohortBasedTemporalCharacterization = runCohortBasedTemporalCharacterization,
          covariateCohortDatabaseSchema = x$cohortDatabaseSchemaFinal,
          covariateCohortIds = covariateCohortIds,
          covariateCohortTable = covariateCohortTable,
          covariateCohortDefinitionSet = covariateCohortDefinitionSet,
          cohortCovariateAnalysisId = cohortCovariateAnalysisId
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
      outputFolder = outputFolder,
      runCohortBasedTemporalCharacterization = runCohortBasedTemporalCharacterization,
      covariateCohortIds = covariateCohortIds,
      covariateCohortTable = covariateCohortTable,
      covariateCohortDefinitionSet = covariateCohortDefinitionSet,
      cohortCovariateAnalysisId = cohortCovariateAnalysisId
    )
    
    ParallelLogger::stopCluster(cluster = cluster)
  }
