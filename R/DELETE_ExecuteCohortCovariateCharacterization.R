#' #' Execute the cohort covariate
#' #'
#' #' @details
#' #' This function executes the cohort covariate
#' #'
#' #' @param connectionDetails                   An object of type \code{connectionDetails} as created
#' #'                                            using the
#' #'                                            \code{\link[DatabaseConnector]{createConnectionDetails}}
#' #'                                            function in the DatabaseConnector package.
#' #' @param cdmDatabaseSchema                   Schema name where your patient-level data in OMOP CDM
#' #'                                            format resides. Note that for SQL Server, this should
#' #'                                            include both the database and schema name, for example
#' #'                                            'cdm_data.dbo'.
#' #' @param targetCohortDatabaseSchema          Schema with instantiated target cohorts.
#' #' @param targetCohortIds                     Target Cohort ids.
#' #' @param covariateCohortDatabaseSchema       Schema with instantiated target cohorts.
#' #' @param covariateCohortIds                  CohortIds of the covariate Cohorts. These must be instantiated
#' #'                                            in covariateCohortDatabaseSchema.
#' #' @param unionCovariateCohorts               Do you want to union the covariateCohorts into one cohort. If yes,
#' #'                                            a new cohort will be created and that cohort will be called Composite
#' #'                                            with the cohortId = -1.
#' #' @param targetCohortTableName               target Cohort Table Names
#' #' @param covariateCohortTableName            Covariate Cohort Table Names
#' #' @param tempEmulationSchema                 Some database platforms like Oracle and Impala do not
#' #'                                            truly support temp tables. To emulate temp tables,
#' #'                                            provide a schema with write privileges where temp tables
#' #'                                            can be created.
#' #' @param outputFolder                        Name of local folder to place results; make sure to use
#' #'                                            forward slashes (/). Do not use a folder on a network
#' #'                                            drive since this greatly impacts performance.
#' #' @param temporalStartDays	                  A list of integers representing the start of a time period,
#' #'                                            relative to the index date. 0 indicates the index date, -1
#' #'                                            indicates the day before the index date, etc. The start day
#' #'                                            is included in the time period.
#' #' @param temporalEndDays	                    A list of integers representing the end of a time period,
#' #'                                            relative to the index date. 0 indicates the index date, -1
#' #'                                            indicates the day before the index date, etc. The end day
#' #'                                            is included in the time period.
#' #' @param minCellCount                        Default 5
#' #' @param databaseId                          The id of the database
#' #' @param notes                               Do you want to add any text notes to metadata output
#' #' @param covariateCohortDefinitionSet        Cohort Definition set object of the covariate cohort
#' #'
#' #' @export
#' executeCohortCovariateCharacterization <-
#'   function(connectionDetails,
#'            cdmDatabaseSchema,
#'            targetCohortDatabaseSchema,
#'            targetCohortIds,
#'            covariateCohortDatabaseSchema,
#'            covariateCohortIds,
#'            unionCovariateCohorts = TRUE,
#'            covariateCohortDefinitionSet = PhenotypeLibrary::getPhenotypeLog() |>
#'              dplyr::filter(.data$cohortId %in% c(covariateCohortIds)) |>
#'              dplyr::select(
#'                .data$cohortId,
#'                .data$cohortName
#'              ),
#'            targetCohortTableName,
#'            cohortCovariateAnalysisId = 150,
#'            covariateCohortTableName,
#'            temporalStartDays = c(-1:1),
#'            temporalEndDays = temporalStartDays,
#'            minCellCount = 5,
#'            minCharacterizationMean = 0.0001,
#'            tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
#'            databaseId = NULL,
#'            notes = NULL,
#'            outputFolder) {
#'     if (!file.exists(outputFolder)) {
#'       dir.create(outputFolder, recursive = TRUE)
#'     }
#'
#'     metaData <- dplyr::tibble(
#'       startDate = Sys.Date() |> as.character(),
#'       startTime = Sys.time() |> as.character(),
#'       databaseId = NA,
#'       notes = NA,
#'       minCellCount = !!minCellCount,
#'       minCharacterizationMean = !!minCharacterizationMean
#'     )
#'
#'     if (!is.null(databaseId)) {
#'       metaData <- metaData |>
#'         dplyr::mutate(
#'           databaseId = !!databaseId
#'         )
#'     }
#'     if (!is.null(notes)) {
#'       metaData <- metaData |>
#'         dplyr::mutate(
#'           notes = !!notes
#'         )
#'     }
#'
#'     readr::write_excel_csv(
#'       x = metaData,
#'       file = file.path(
#'         outputFolder,
#'         "metaData.csv"
#'       )
#'     )
#'
#'     ParallelLogger::addDefaultFileLogger(file.path(outputFolder, "log.txt"))
#'     ParallelLogger::addDefaultErrorReportLogger(file.path(outputFolder, "errorReportR.txt"))
#'     on.exit(ParallelLogger::unregisterLogger("DEFAULT_FILE_LOGGER", silent = TRUE))
#'     on.exit(
#'       ParallelLogger::unregisterLogger("DEFAULT_ERRORREPORT_LOGGER", silent = TRUE),
#'       add = TRUE
#'     )
#'
#'     connection <-
#'       DatabaseConnector::connect(connectionDetails = connectionDetails)
#'
#'     ParallelLogger::logInfo("Running Feature Extraction using Covariate Cohorts")
#'
#'     if (unionCovariateCohorts) {
#'       randomTableNameForUnionCohort <-
#'         paste0("#", generateRandomString())
#'
#'       ParallelLogger::logInfo(" - Creating union of covariate cohorts")
#'       CohortAlgebra::unionCohorts(
#'         connection = connection,
#'         sourceCohortDatabaseSchema = covariateCohortDatabaseSchema,
#'         sourceCohortTable = covariateCohortTableName,
#'         targetCohortDatabaseSchema = NULL,
#'         isTempTable = TRUE,
#'         targetCohortTable = randomTableNameForUnionCohort,
#'         oldToNewCohortId = dplyr::tibble(
#'           oldCohortId = covariateCohortIds,
#'           newCohortId = -1
#'         ),
#'         tempEmulationSchema = tempEmulationSchema
#'       )
#'
#'
#'       randomTableNameForCovariateCohort <-
#'         paste0("#", generateRandomString())
#'       ParallelLogger::logInfo(" - Copying covariate cohorts")
#'       CohortAlgebra::copyCohorts(
#'         connection = connection,
#'         oldToNewCohortId = dplyr::tibble(
#'           oldCohortId = covariateCohortIds,
#'           newCohortId = covariateCohortIds
#'         ),
#'         sourceCohortDatabaseSchema = covariateCohortDatabaseSchema,
#'         sourceCohortTable = covariateCohortTableName,
#'         targetCohortDatabaseSchema = NULL,
#'         targetCohortTable = randomTableNameForCovariateCohort,
#'         isTempTable = TRUE,
#'         tempEmulationSchema = tempEmulationSchema
#'       )
#'
#'       finalTempTable <-
#'         paste0("#", generateRandomString())
#'
#'       ParallelLogger::logInfo(" - Setting up covariate cohorts")
#'       CohortAlgebra::appendCohortTables(
#'         connection = connection,
#'         sourceTables = dplyr::tibble(
#'           sourceCohortDatabaseSchema = NA,
#'           sourceCohortTableName = c(
#'             randomTableNameForCovariateCohort,
#'             randomTableNameForUnionCohort
#'           )
#'         ),
#'         targetCohortDatabaseSchema = NULL,
#'         targetCohortTable = finalTempTable,
#'         isTempTable = TRUE
#'       )
#'
#'       covariateCohortDefinitionSet <-
#'         dplyr::bind_rows(
#'           covariateCohortDefinitionSet,
#'           dplyr::tibble(
#'             cohortId = -1,
#'             cohortName = "Composite"
#'           )
#'         )
#'
#'       cohortDomainSettings <-
#'         FeatureExtraction::createCohortBasedTemporalCovariateSettings(
#'           analysisId = cohortCovariateAnalysisId,
#'           covariateCohortDatabaseSchema = NULL,
#'           covariateCohortTable = finalTempTable,
#'           covariateCohorts = covariateCohortDefinitionSet |>
#'             dplyr::filter(.data$cohortId %in% c(covariateCohortIds)),
#'           valueType = "binary",
#'           temporalStartDays = temporalStartDays,
#'           temporalEndDays = temporalEndDays
#'         )
#'     } else {
#'       cohortDomainSettings <-
#'         FeatureExtraction::createCohortBasedTemporalCovariateSettings(
#'           analysisId = cohortCovariateAnalysisId,
#'           covariateCohortDatabaseSchema = covariateCohortDatabaseSchema,
#'           covariateCohortTable = covariateCohortTableName,
#'           covariateCohorts = covariateCohortDefinitionSet |>
#'             dplyr::filter(.data$cohortId %in% c(covariateCohortIds)),
#'           valueType = "binary",
#'           temporalStartDays = temporalStartDays,
#'           temporalEndDays = temporalEndDays
#'         )
#'     }
#'     ParallelLogger::logInfo(" - Beginning Feature Extraction")
#'     featureExtractionOutput <-
#'       CohortDiagnostics:::getCohortCharacteristics(
#'         connection = connection,
#'         cdmDatabaseSchema = cdmDatabaseSchema,
#'         cohortDatabaseSchema = targetCohortDatabaseSchema,
#'         cohortTable = targetCohortTableName,
#'         cohortIds = targetCohortIds,
#'         covariateSettings = cohortDomainSettings,
#'         exportFolder = outputFolder
#'       )
#'
#'     featureExtractionOutputFinal <- c()
#'     for (i in (1:length(names(featureExtractionOutput)))) {
#'       name <- names(featureExtractionOutput)[[i]]
#'       featureExtractionOutputFinal[[name]] <- featureExtractionOutput[[name]] |>
#'         dplyr::collect()
#'       if (name == "covariates") {
#'         featureExtractionOutputFinal[[name]] <- featureExtractionOutputFinal[[name]] |>
#'           dplyr::collect() |>
#'           dplyr::filter(.data$sumValue > minCellCount) |>
#'           dplyr::filter(.data$mean >= minCharacterizationMean)
#'       }
#'       if (name == "covariatesContinuous") {
#'         featureExtractionOutputFinal[[name]] <-
#'           featureExtractionOutputFinal[[name]] |>
#'           dplyr::collect() |>
#'           dplyr::filter(.data$countValue > minCellCount) |>
#'           dplyr::filter(.data$mean >= minCharacterizationMean)
#'       }
#'     }
#'
#'     saveRDS(
#'       object = featureExtractionOutputFinal,
#'       file = file.path(outputFolder, "FeatureExtraction.RDS")
#'     )
#'
#'     DatabaseConnector::disconnect(connection = connection)
#'
#'     return(featureExtractionOutputFinal)
#'   }
#'
#'
#' #'
#' #' @export
#' executeCohortCovariateCharacterizationInParallel <-
#'   function(cdmSources,
#'            targetCohortIds,
#'            covariateCohortIds,
#'            unionCovariateCohorts = TRUE,
#'            targetCohortDatabaseSchema = NULL,
#'            targetCohortTableName,
#'            cohortCovariateAnalysisId = 150,
#'            covariateCohortDefinitionSet,
#'            covariateCohortDatabaseSchema = targetCohortDatabaseSchema,
#'            covariateCohortTableName = targetCohortTableName,
#'            userService = "OHDSI_USER",
#'            passwordService = "OHDSI_PASSWORD",
#'            databaseIds = getListOfDatabaseIds(),
#'            temporalStartDays,
#'            sequence = 1,
#'            temporalEndDays,
#'            maxCores = parallel::detectCores() /
#'              3,
#'            minCellCount = 5,
#'            minCharacterizationMean = 0.0001,
#'            notes = NULL,
#'            tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
#'            outputFolder) {
# cdmSources <-
#   getCdmSource(cdmSources = cdmSources,
#                database = databaseIds,
#                sequence = sequence)
#'
#'     x <- list()
#'     for (i in 1:nrow(cdmSources)) {
#'       x[[i]] <- cdmSources[i, ]
#'     }
#'
#'     # use Parallel Logger to run in parallel
#'     cluster <-
#'       ParallelLogger::makeCluster(
#'         numberOfThreads = min(
#'           as.integer(trunc(
#'             parallel::detectCores() /
#'               2
#'           )),
#'           nrow(cdmSources)
#'         ),
#'         maxCores
#'       )
#'
#'     ## file logger
#'     loggerName <-
#'       paste0(
#'         "CCo_",
#'         stringr::str_replace_all(
#'           string = Sys.time(),
#'           pattern = ":|-|EDT| ",
#'           replacement = ""
#'         )
#'       )
#'
#'     dir.create(path = outputFolder, showWarnings = FALSE, recursive = TRUE)
#'
#'     ParallelLogger::addDefaultFileLogger(fileName = file.path(outputFolder, paste0(loggerName, ".txt")))
#'
#'     executeCohortCovariateCharacterizationX <-
#'       function(x,
#'                targetCohortIds,
#'                covariateCohortIds,
#'                unionCovariateCohorts,
#'                targetCohortTableName,
#'                targetCohortDatabaseSchema = NULL,
#'                cohortCovariateAnalysisId,
#'                userService,
#'                passwordService,
#'                databaseIds,
#'                covariateCohortDefinitionSet,
#'                covariateCohortTableName = NULL,
#'                covariateCohortDatabaseSchema = NULL,
#'                temporalStartDays,
#'                temporalEndDays,
#'                notes,
#'                minCellCount,
#'                minCharacterizationMean,
#'                tempEmulationSchema,
#'                outputFolder) {
#'         connectionDetails <- DatabaseConnector::createConnectionDetails(
#'           dbms = x$dbms,
#'           user = keyring::key_get(userService),
#'           password = keyring::key_get(passwordService),
#'           server = x$serverFinal,
#'           port = x$port
#'         )
#'
#'         cohortCovariateCharacterizationOutputFolder <-
#'           file.path(outputFolder, x$sourceKey)
#'
#'         dir.create(
#'           path = cohortCovariateCharacterizationOutputFolder,
#'           showWarnings = FALSE,
#'           recursive = TRUE
#'         )
#'
#'         if (is.null(targetCohortDatabaseSchema)) {
#'           targetCohortDatabaseSchema <- x$cohortDatabaseSchemaFinal
#'         }
#'
#'         if (is.null(covariateCohortDatabaseSchema)) {
#'           covariateCohortDatabaseSchema <- targetCohortDatabaseSchema
#'         }
#'
#'         if (is.null(covariateCohortTableName)) {
#'           covariateCohortTableName <- paste0(
#'             "pl_",
#'             x$sourceKey
#'           )
#'         }
#'
#'         featureExtractionOutput <-
#'           executeCohortCovariateCharacterization(
#'             connectionDetails = connectionDetails,
#'             cdmDatabaseSchema = x$cdmDatabaseSchemaFinal,
#'             targetCohortDatabaseSchema = targetCohortDatabaseSchema,
#'             targetCohortIds = targetCohortIds,
#'             targetCohortTableName = targetCohortTableName,
#'             covariateCohortIds = covariateCohortIds,
#'             covariateCohortDefinitionSet = covariateCohortDefinitionSet,
#'             covariateCohortDatabaseSchema = covariateCohortDatabaseSchema,
#'             covariateCohortTableName = covariateCohortTableName,
#'             unionCovariateCohorts = unionCovariateCohorts,
#'             temporalStartDays = temporalStartDays,
#'             temporalEndDays = temporalEndDays,
#'             minCellCount = minCellCount,
#'             minCharacterizationMean = minCharacterizationMean,
#'             notes = notes,
#'             databaseId = x$sourceKey,
#'             outputFolder = cohortCovariateCharacterizationOutputFolder
#'           )
#'         rm("featureExtractionOutput")
#'       }
#'
#'     ParallelLogger::clusterApply(
#'       cluster = cluster,
#'       x = x,
#'       fun = executeCohortCovariateCharacterizationX,
#'       targetCohortIds = targetCohortIds,
#'       covariateCohortIds = covariateCohortIds,
#'       unionCovariateCohorts = unionCovariateCohorts,
#'       targetCohortDatabaseSchema = targetCohortDatabaseSchema,
#'       targetCohortTableName = targetCohortTableName,
#'       cohortCovariateAnalysisId = cohortCovariateAnalysisId,
#'       userService = userService,
#'       passwordService = passwordService,
#'       databaseIds = databaseIds,
#'       covariateCohortDefinitionSet = covariateCohortDefinitionSet,
#'       covariateCohortDatabaseSchema = covariateCohortDatabaseSchema,
#'       covariateCohortTableName = covariateCohortTableName,
#'       temporalStartDays = temporalStartDays,
#'       temporalEndDays = temporalEndDays,
#'       tempEmulationSchema = tempEmulationSchema,
#'       outputFolder = outputFolder,
#'       minCellCount = minCellCount,
#'       minCharacterizationMean = minCharacterizationMean,
#'       notes = notes
#'     )
#'
#'     ParallelLogger::stopCluster(cluster = cluster)
#'   }
