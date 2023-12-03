#' #' @export
#' #'
#' runPheValuatorWorkflow <- function(x,
#'                                    rootFolder,
#'                                    projectCode,
#'                                    cohortDefinitionSet,
#'                                    candidateCohortIds,
#'                                    phenotypeName,
#'                                    cdmDatabaseSchema,
#'                                    cohortDatabaseSchema,
#'                                    cohortTableName,
#'                                    xSensCohortId,
#'                                    xSpecCohortId,
#'                                    daysFromxSpec,
#'                                    prevalenceCohortId,
#'                                    evaluationPopulationCohortId,
#'                                    excludedCovariateConceptIds,
#'                                    addDescendantsToExclude = TRUE,
#'                                    startDayWindow1 = 0,
#'                                    endDayWindow1 = 30,
#'                                    startDayWindow2 = 31,
#'                                    endDayWindow2 = 60,
#'                                    startDayWindow3 = 61,
#'                                    endDayWindow3 = 365,
#'                                    xSpecCohortSize = 5000,
#'                                    modelBaseSampleSize = 15000,
#'                                    baseSampleSize = 200000,
#'                                    startDate = "20100101",
#'                                    endDate = "21000101",
#'                                    cutPoints =  c("EV"),
#'                                    defaultWashoutPeriod = 0,
#'                                    defaultSplayPrior = 7,
#'                                    defaultSplayPost = 7,
#'                                    candidateCohortDefinitionSet,
#'                                    resultsFolder,
#'                                    databaseId,
#'                                    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
#'   # fullCohortDefinitionSet <-
#'   #   getCohortDefinitionSet(
#'   #     candidateCohortDefinitionSet = cohortDefinitionSet %>%
#'   #       dplyr::filter(cohortId %in% c(candidateCohortIds)),
#'   #     phenotypeName = phenotypeName
#'   #   )
#'   
#'   cohortDatabaseSchema = x$cdmSource$cohortDatabaseSchemaFinal
#'   cdmDatabaseSchema <- x$cdmSource$cdmDatabaseSchemaFinal
#'   databaseId <- x$cdmSource$sourceKey
#'   sourceName <- x$cdmSource$sourceName
#'   
#'   message <- paste0(
#'     "Running ",
#'     sourceName,
#'     " on ",
#'     x$cdmSource$runOn,
#'     "\n     server: ",
#'     x$cdmSource$serverFinal,
#'     "\n     cdmDatabaseSchema: ",
#'     cdmDatabaseSchema,
#'     "\n     cohortDatabaseSchema: ",
#'     cohortDatabaseSchema
#'   )
#'   outputFolder <- file.path(rootFolder, projectCode)
#'   resultsFolder <- file.path(outputFolder, x$cdmSource$sourceKey)
#'   cohortGeneratorIncrementalFolder <-
#'     file.path(resultsFolder, "CohortGenerator")
#'   dir.create(path = cohortGeneratorIncrementalFolder,
#'              showWarnings = FALSE,
#'              recursive = TRUE)
#'   
#'   ParallelLogger::clearLoggers()
#'   ParallelLogger::addDefaultFileLogger(fileName = file.path(
#'     file.path(outputFolder),
#'     paste0("log_run_phevaluator_",
#'            x$cdmSource$sourceKey,
#'            ".txt")
#'   ))
#'   
#'   ParallelLogger::logInfo(message)
#'   
#'   connectionDetails <- DatabaseConnector::createConnectionDetails(
#'     dbms = x$cdmSource$dbms,
#'     user = keyring::key_get("OHDSI_USER"),
#'     password = keyring::key_get("OHDSI_PASSWORD"),
#'     server = x$cdmSource$serverFinal,
#'     port = x$cdmSource$port
#'   )
#'   
#'   # CohortGenerator::createCohortTables(
#'   #   connectionDetails = connectionDetails,
#'   #   cohortTableNames = cohortTableNames,
#'   #   cohortDatabaseSchema = cohortDatabaseSchema,
#'   #   incremental = TRUE
#'   # )
#'   # 
#'   # connection <-
#'   #   DatabaseConnector::connect(connectionDetails = connectionDetails)
#'   # 
#'   # CohortGenerator::generateCohortSet(
#'   #   connection = connection,
#'   #   cdmDatabaseSchema = cdmDatabaseSchema,
#'   #   tempEmulationSchema = tempEmulationSchema,
#'   #   cohortDatabaseSchema = cohortDatabaseSchema,
#'   #   cohortTableNames = cohortTableNames,
#'   #   cohortDefinitionSet = cohortDefinitionSet,
#'   #   incremental = TRUE,
#'   #   incrementalFolder = cohortGeneratorIncrementalFolder
#'   # )
#'   
#'   # saveCandidateCohortData(
#'   #   connectionDetails = connectionDetails,
#'   #   cohortDatabaseSchema = cohortDatabaseSchema,
#'   #   cohortTable = cohortTableNames$cohortTable,
#'   #   candidateCohortId = candidateCohortIds,
#'   #   resultsFolder = resultsFolder
#'   # )
#'   
#'   debug(executePheValuator)
#'   executePheValuator(
#'     connectionDetails = connectionDetails,
#'     cdmDatabaseSchema = cdmDatabaseSchema,
#'     candidateCohortDefinitionSet = cohortDefinitionSet,
#'     cohortDatabaseSchema = cohortDatabaseSchema,
#'     cohortTableName = cohortTableName,
#'     excludedCovariateConceptIds = excludedCovariateConceptIds,
#'     resultsFolder = resultsFolder,
#'     databaseId = databaseId,
#'     tempEmulationSchema = tempEmulationSchema,
#'     xSensCohortId = xSensCohortId,
#'     xSpecCohortId = xSpecCohortId,
#'     prevalenceCohortId = prevalenceCohortId,
#'     startDayWindow1 = startDayWindow1,
#'     endDayWindow1 = endDayWindow1,
#'     startDayWindow2 = startDayWindow2,
#'     endDayWindow2 = endDayWindow2,
#'     startDayWindow3 = startDayWindow3,
#'     endDayWindow3 = endDayWindow3,
#'     xSpecCohortSize = xSpecCohortSize,
#'     modelBaseSampleSize = modelBaseSampleSize,
#'     baseSampleSize = baseSampleSize,
#'     startDate = startDate,
#'     endDate = endDate,
#'     daysFromxSpec = daysFromxSpec
#'   )
#'   
#'   # for (k in (1:nrow(fullCohortDefinitionSet))) {
#'   #   CohortExplorer::createCohortExplorerApp(
#'   #     connectionDetails = connectionDetails,
#'   #     cohortDatabaseSchema = cohortDatabaseSchema,
#'   #     cdmDatabaseSchema = cdmDatabaseSchema,
#'   #     cohortTable = cohortTableNames$cohortTable,
#'   #     cohortDefinitionId = fullCohortDefinitionSet[k,]$cohortId,
#'   #     cohortName = fullCohortDefinitionSet[k,]$cohortName,
#'   #     exportFolder = file.path(outputFolder),
#'   #     databaseId = databaseId %>% SqlRender::snakeCaseToCamelCase()
#'   #   )
#'   # }
#'   
#'   # CohortDiagnostics::executeDiagnostics(
#'   #   cohortDefinitionSet = fullCohortDefinitionSet,
#'   #   connectionDetails = connectionDetails,
#'   #   exportFolder = file.path(resultsFolder, "CohortDiagnostics"),
#'   #   databaseId = databaseId,
#'   #   cohortTableNames = cohortTableNames,
#'   #   cdmDatabaseSchema = cdmDatabaseSchema,
#'   #   cohortDatabaseSchema = cohortDatabaseSchema,
#'   #   runInclusionStatistics = FALSE,
#'   #   runIncludedSourceConcepts = TRUE,
#'   #   runOrphanConcepts = TRUE,
#'   #   runTimeSeries = FALSE,
#'   #   runVisitContext = TRUE,
#'   #   runBreakdownIndexEvents = TRUE,
#'   #   runIncidenceRate = TRUE,
#'   #   runCohortRelationship = TRUE,
#'   #   runTemporalCohortCharacterization = TRUE,
#'   #   minCellCount = 0,
#'   #   minCharacterizationMean = 0,
#'   #   incremental = FALSE
#'   # )
#'   # 
#'   # CohortDiagnostics::createMergedResultsFile(
#'   #   dataFolder = file.path(resultsFolder, "CohortDiagnostics"),
#'   #   sqliteDbPath = file.path(
#'   #     resultsFolder,
#'   #     "CohortDiagnostics",
#'   #     "MergedCohortDiagnosticsData.sqlite"
#'   #   ),
#'   #   overwrite = TRUE
#'   # )
#' }
