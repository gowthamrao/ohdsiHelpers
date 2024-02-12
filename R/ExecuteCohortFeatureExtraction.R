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
#' @param aggregated                          Should aggregate statistics be computed instead of covariates per cohort entry?
#'
#' @export
executeCohortFeatureExtraction <-
  function(connectionDetails,
           cdmDatabaseSchema,
           vocabularyDatabaseSchema = cdmDatabaseSchema,
           cohortDatabaseSchema,
           cohortIds,
           cohortTable,
           covariateCohortDatabaseSchema = cohortDatabaseSchema,
           covariateCohortIds = covariateCohortDefinitionSet$cohortId,
           covariateCohortTable = cohortTable,
           covariateCohortDefinitionSet,
           cohortCovariateAnalysisId = 150,
           temporalStartDays = c(-1:1),
           temporalEndDays = temporalStartDays,
           minCellCount = 5,
           aggregated= TRUE,
           minCharacterizationMean = 0.0001,
           tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
           outputFolder = NULL) {
    
    if (!aggregated) {
      if (!length(cohortIds) == 1) {
        stop("When aggregate = FALSE, only calculation is done one cohort at a time.")
      }
    }
    
    if (!is.null(outputFolder)) {
      if (!file.exists(outputFolder)) {
        dir.create(outputFolder, recursive = TRUE)
      }
    }
    
    cohortDomainSettings <-
      FeatureExtraction::createCohortBasedTemporalCovariateSettings(
        analysisId = cohortCovariateAnalysisId,
        covariateCohortDatabaseSchema = covariateCohortDatabaseSchema,
        covariateCohortTable = covariateCohortTable,
        covariateCohorts = covariateCohortDefinitionSet |>
          dplyr::filter(.data$cohortId %in% c(covariateCohortIds)) |>
          dplyr::select(cohortId,
                        cohortName),
        valueType = "binary",
        temporalStartDays = temporalStartDays,
        temporalEndDays = temporalEndDays
      )
    
    connection <-
      DatabaseConnector::connect(connectionDetails = connectionDetails)

    featureExtractionOutput <-
      FeatureExtraction::getDbCovariateData(
        connection = connection,
        oracleTempSchema = tempEmulationSchema,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortIds = cohortIds,
        covariateSettings = cohortDomainSettings,
        aggregated = aggregated,
        cohortTable = cohortTable,
        cohortDatabaseSchema = cohortDatabaseSchema
      )
    
    DatabaseConnector::disconnect(connection = connection)
    
    if (!is.null(outputFolder)) {
      unlink(
        file.path(outputFolder, "covariateData"),
        recursive = TRUE,
        force = TRUE
      )
      FeatureExtraction::saveCovariateData(covariateData = featureExtractionOutput,
                                           file = file.path(outputFolder, "covariateData"))
    } else {
      return(featureExtractionOutput)
    }
  }


#'
#' @export
executeCohortFeatureExtractionInParallel <-
  function(cdmSources,
           cohortIds,
           cohortTable,
           userService = "OHDSI_USER",
           passwordService = "OHDSI_PASSWORD",
           databaseIds = getListOfDatabaseIds(),
           covariateCohortIds = covariateCohortDefinitionSet$cohortId,
           covariateCohortTable = cohortTable,
           covariateCohortDefinitionSet,
           cohortCovariateAnalysisId = 150,
           temporalStartDays = c(-1:1),
           temporalEndDays = temporalStartDays,
           minCellCount = 5,
           minCharacterizationMean = 0.0001,
           sequence = 1,
           aggregated = TRUE,
           maxCores = parallel::detectCores() /
             3,
           tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
           outputFolder) {
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
    
    executeCohortFeatureExtractionX <-
      function(x,
               cohortIds,
               cohortTable,
               userService,
               passwordService,
               tempEmulationSchema,
               covariateSettings,
               covariateCohortIds,
               covariateCohortTable,
               covariateCohortDefinitionSet,
               cohortCovariateAnalysisId,
               temporalStartDays,
               temporalEndDays,
               minCellCount,
               minCharacterizationMean,
               outputFolder,
               aggregated) {
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
        
        executeCohortFeatureExtraction(
          connectionDetails = connectionDetails,
          cdmDatabaseSchema = x$cdmDatabaseSchemaFinal,
          cohortDatabaseSchema = x$cohortDatabaseSchemaFinal,
          cohortIds = cohortIds,
          cohortTable = cohortTable,
          outputFolder = outputFolder,
          covariateCohortDatabaseSchema = x$cohortDatabaseSchemaFinal,
          covariateCohortIds = covariateCohortIds,
          covariateCohortTable = covariateCohortTable,
          covariateCohortDefinitionSet = covariateCohortDefinitionSet,
          cohortCovariateAnalysisId = cohortCovariateAnalysisId,
          temporalStartDays = temporalStartDays,
          temporalEndDays = temporalEndDays,
          minCharacterizationMean = minCharacterizationMean,
          aggregated = aggregated
        )
      }
    
    ParallelLogger::clusterApply(
      cluster = cluster,
      x = x,
      fun = executeCohortFeatureExtractionX,
      cohortIds = cohortIds,
      cohortTable = cohortTable,
      userService = userService,
      passwordService = passwordService,
      covariateSettings = covariateSettings,
      tempEmulationSchema = tempEmulationSchema,
      outputFolder = outputFolder,
      covariateCohortIds = covariateCohortIds,
      covariateCohortTable = covariateCohortTable,
      covariateCohortDefinitionSet = covariateCohortDefinitionSet,
      cohortCovariateAnalysisId = cohortCovariateAnalysisId,
      temporalStartDays = temporalStartDays,
      temporalEndDays = temporalEndDays,
      minCharacterizationMean = minCharacterizationMean,
      aggregated = aggregated
    )
    
    ParallelLogger::stopCluster(cluster = cluster)
  }
