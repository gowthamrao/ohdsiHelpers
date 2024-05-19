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
  function(connectionDetails = NULL,
           connection = NULL,
           cdmDatabaseSchema,
           vocabularyDatabaseSchema = cdmDatabaseSchema,
           cohortDatabaseSchema,
           cohortIds,
           cohortTable,
           covariateSettings = NULL,
           addCohortBasedTemporalCovariateSettings = TRUE,
           removeNonCohortBasedCovariateSettings = FALSE,
           covariateCohortDatabaseSchema = cohortDatabaseSchema,
           covariateCohortIds = NULL,
           covariateCohortTable = cohortTable,
           covariateCohortDefinitionSet = NULL,
           cohortCovariateAnalysisId = 150,
           tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
           outputFolder = NULL,
           aggregated = TRUE,
           rowIdField = "subject_id",
           incremental = TRUE) {
    
    if (!is.null(outputFolder)) {
      if (!file.exists(outputFolder)) {
        dir.create(outputFolder, recursive = TRUE)
      }
    }
    
    if (!aggregated) {
      if (length(cohortIds) > 1) {
        stop("only one cohort id allowed when doing aggregated = FALSE")
      }
    }
    
    if (addCohortBasedTemporalCovariateSettings) {
      if (is.null(covariateSettings$temporal)) {
        stop(
          "covariateSettings is not temporal. Cannot add cohort based temporal covariate settings."
        )
      }
      if (covariateSettings$temporal) {
        if (is.null(covariateCohortDefinitionSet)) {
          stop(
            "covariateCohortDefinitionSet is NULL. cannot run cohort temporal characterization"
          )
        }
        
        if (!is.null(covariateCohortIds)) {
          covariateCohortDefinitionSet <- covariateCohortDefinitionSet |>
            dplyr::filter(.data$cohortId %in% covariateCohortIds)
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
            covariateCohortDefinitionSet$cohortId |>
            unique() |>
            sort()
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
            temporalEndDays = temporalEndDays,
            includedCovariateIds = covariateCohortIds
          )
        
        if (removeNonCohortBasedCovariateSettings) {
          covariateSettings <- cohortBasedTemporalCovariateSettings
        } else {
          covariateSettings <- list(covariateSettings,
                                    cohortBasedTemporalCovariateSettings)
        }
      } else {
        stop(
          "Cant run cohort based temporal characterization because covariate setting is not temporal"
        )
      }
    }
    
    if (!is.null(outputFolder)) {
      ParallelLogger::addDefaultFileLogger(file.path(outputFolder, "log.txt"))
      ParallelLogger::addDefaultErrorReportLogger(file.path(outputFolder, "errorReportR.txt"))
      on.exit(ParallelLogger::unregisterLogger("DEFAULT_FILE_LOGGER", silent = TRUE))
      on.exit(
        ParallelLogger::unregisterLogger("DEFAULT_ERRORREPORT_LOGGER", silent = TRUE),
        add = TRUE
      )
    }
    
    if (is.null(connection)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    }
    
    useRowId <- (rowIdField == "row_id")
    if (useRowId) {
      sql <- "
      DROP TABLE  IF EXISTS #cohort_person;

      SELECT ROW_NUMBER() OVER (ORDER BY subject_id, cohort_start_date) AS row_id,
              cohort_definition_id,
              subject_id,
              cohort_start_date,
              cohort_end_date
      INTO #cohort_person
      FROM @cohort_database_schema.@cohort_table
      WHERE cohort_definition_id IN (@target_cohort_id);

      "
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = sql,
        cohort_database_schema = cohortDatabaseSchema,
        cohort_table = cohortTable,
        target_cohort_id = cohortIds,
        profile = FALSE,
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
      cohortDatabaseSchema <- NULL
      cohortTable <- "#cohort_person"
    }
    
    ParallelLogger::logInfo(" - Beginning Feature Extraction")
    
    for (x in (1:length(cohortIds))) {
      cohortId <- cohortIds[[x]]
      ParallelLogger::logInfo(paste0("   - cohort id: ", cohortId))
      
      skipCohort <- FALSE
      if (incremental) {
        if (file.exists(file.path(outputFolder, cohortId))) {
          skipCohort <- TRUE
        } 
      }
      
      if (!skipCohort) {
        covariateData <-
          FeatureExtraction::getDbCovariateData(
            connection = connection,
            oracleTempSchema = tempEmulationSchema,
            cdmDatabaseSchema = cdmDatabaseSchema,
            cohortDatabaseSchema = cohortDatabaseSchema,
            cdmVersion = 5,
            cohortTable = cohortTable,
            cohortIds = cohortId,
            covariateSettings = covariateSettings,
            aggregated = aggregated,
            cohortTableIsTemp = is.null(cohortDatabaseSchema),
            rowIdField = rowIdField
          )
        
        if (!is.null(outputFolder)) {
          dir.create(
            path = file.path(outputFolder),
            showWarnings = FALSE,
            recursive = TRUE
          )
          FeatureExtraction::saveCovariateData(covariateData = covariateData,
                                               file = file.path(outputFolder, cohortId))
        }
      } else {
        ParallelLogger::logInfo(paste0("    - skipping cohort id: ", cohortId))
      }
    }
    
    if (useRowId) {
      sql <- "DROP TABLE  IF EXISTS #cohort_person;"
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = sql,
        profile = FALSE,
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
    }
    
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
           covariateSettings = OhdsiHelpers::getFeatureExtractionDefaultTemporalCovariateSettings(),
           addCohortBasedTemporalCovariateSettings = FALSE,
           removeNonCohortBasedCovariateSettings = FALSE,
           covariateCohortIds = NULL,
           covariateCohortTable = cohortTable,
           covariateCohortDefinitionSet = NULL,
           cohortCovariateAnalysisId = 150,
           sequence = 1,
           maxCores = parallel::detectCores() /
             3,
           tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
           outputFolder,
           rowIdField = "subject_id",
           aggregated = TRUE,
           incremental = TRUE) {

    cdmSources <-
      getCdmSource(cdmSources = cdmSources,
                   database = databaseIds,
                   sequence = sequence)
    
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
    
    executeFeatureExtractionX <-
      function(x,
               cohortIds,
               cohortTable,
               userService,
               passwordService,
               tempEmulationSchema,
               covariateSettings,
               addCohortBasedTemporalCovariateSettings,
               removeNonCohortBasedCovariateSettings,
               covariateCohortIds,
               covariateCohortTable,
               covariateCohortDefinitionSet,
               cohortCovariateAnalysisId,
               outputFolder,
               rowIdField,
               aggregated,
               incremental) {
        connectionDetails <- DatabaseConnector::createConnectionDetails(
          dbms = x$dbms,
          user = keyring::key_get(userService),
          password = keyring::key_get(passwordService),
          server = x$serverFinal,
          port = x$port
        )
        
        outputFolder <-
          file.path(outputFolder, x$sourceKey)
        
        executeFeatureExtraction(
          connectionDetails = connectionDetails,
          cdmDatabaseSchema = x$cdmDatabaseSchemaFinal,
          cohortDatabaseSchema = x$cohortDatabaseSchemaFinal,
          cohortIds = cohortIds,
          cohortTable = cohortTable,
          covariateSettings = covariateSettings,
          outputFolder = outputFolder,
          addCohortBasedTemporalCovariateSettings = addCohortBasedTemporalCovariateSettings,
          removeNonCohortBasedCovariateSettings = removeNonCohortBasedCovariateSettings,
          covariateCohortDatabaseSchema = x$cohortDatabaseSchemaFinal,
          covariateCohortIds = covariateCohortIds,
          covariateCohortTable = covariateCohortTable,
          covariateCohortDefinitionSet = covariateCohortDefinitionSet,
          cohortCovariateAnalysisId = cohortCovariateAnalysisId,
          rowIdField = rowIdField,
          aggregated = aggregated,
          incremental = incremental
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
      addCohortBasedTemporalCovariateSettings = addCohortBasedTemporalCovariateSettings,
      removeNonCohortBasedCovariateSettings = removeNonCohortBasedCovariateSettings,
      covariateCohortIds = covariateCohortIds,
      covariateCohortTable = covariateCohortTable,
      covariateCohortDefinitionSet = covariateCohortDefinitionSet,
      cohortCovariateAnalysisId = cohortCovariateAnalysisId,
      rowIdField = rowIdField,
      aggregated = aggregated,
      incremental = incremental
    )
    
    ParallelLogger::stopCluster(cluster = cluster)
  }
