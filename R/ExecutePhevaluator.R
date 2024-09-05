#' #'
#' #' @export
#' getPheValuatorAnalysisList <-
#'   function(xSensCohortId = prevalenceCohortId,
#'            xSpecCohortId,
#'            daysFromxSpec,
#'            prevalenceCohortId,
#'            excludedCovariateConceptIds = NULL,
#'            addDescendantsToExclude = TRUE,
#'            startDayWindow1 = 0,
#'            endDayWindow1 = 30,
#'            startDayWindow2 = 31,
#'            endDayWindow2 = 60,
#'            startDayWindow3 = 61,
#'            endDayWindow3 = 365,
#'            xSpecCohortSize = 5000,
#'            modelBaseSampleSize = 25000,
#'            baseSampleSize = 200000,
#'            startDate = "20100101",
#'            endDate = "21000101",
#'            cutPoints = c("EV"),
#'            defaultWashoutPeriod = 0,
#'            defaultSplayPrior = 7,
#'            defaultSplayPost = 7,
#'            cohortDefinitionSet,
#'            falsePositiveNegativeSubjects = 25,
#'            outputFolder) {
#'     cohortDefinitionSet <- cohortDefinitionSet %>%
#'       dplyr::mutate(compositeName = paste0(
#'         "C",
#'         cohortId,
#'         "_",
#'         cohortName
#'       ))
#'
#'     # Create analysis settings ----------------------------------------------------------------------------------------
#'     covSettings <-
#'       PheValuator::createDefaultCovariateSettings(
#'         excludedCovariateConceptIds = excludedCovariateConceptIds,
#'         includedCovariateIds = c(),
#'         includedCovariateConceptIds = c(),
#'         addDescendantsToExclude = addDescendantsToExclude,
#'         startDayWindow1 = startDayWindow1,
#'         endDayWindow1 = endDayWindow1,
#'         startDayWindow2 = startDayWindow2,
#'         endDayWindow2 = endDayWindow2,
#'         startDayWindow3 = startDayWindow3,
#'         endDayWindow3 = endDayWindow3
#'       )
#'
#'     cohortArgsAcute <-
#'       PheValuator::createCreateEvaluationCohortArgs(
#'         xSpecCohortId = xSpecCohortId,
#'         daysFromxSpec = daysFromxSpec,
#'         xSensCohortId = xSensCohortId,
#'         prevalenceCohortId = prevalenceCohortId,
#'         modelBaseSampleSize = modelBaseSampleSize,
#'         xSpecCohortSize = xSpecCohortSize,
#'         covariateSettings = covSettings,
#'         baseSampleSize = baseSampleSize,
#'         # normally set to 2000000
#'         startDate = startDate,
#'         endDate = endDate,
#'         excludeModelFromEvaluation = FALSE,
#'         falsePositiveNegativeSubjects = falsePositiveNegativeSubjects
#'       )
#'
#'     pheValuatorAnalysisList <- c()
#'
#'     for (i in (1:nrow(cohortDefinitionSet))) {
#'       conditionAlgTestArgs <-
#'         PheValuator::createTestPhenotypeAlgorithmArgs(
#'           cutPoints = cutPoints,
#'           phenotypeCohortId = cohortDefinitionSet[i, ]$cohortId,
#'           washoutPeriod = if (is.null(cohortDefinitionSet[i, ]$washoutPeriod)) {
#'             defaultWashoutPeriod
#'           } else {
#'             cohortDefinitionSet[i, ]$washoutPeriod
#'           },
#'           splayPrior = if (is.null(cohortDefinitionSet[i, ]$splayPrior)) {
#'             defaultSplayPrior
#'           } else {
#'             cohortDefinitionSet[i, ]$splayPrior
#'           },
#'           splayPost = if (is.null(cohortDefinitionSet[i, ]$splayPost)) {
#'             defaultSplayPost
#'           } else {
#'             cohortDefinitionSet[i, ]$splayPost
#'           }
#'         )
#'
#'       analysis <-
#'         PheValuator::createPheValuatorAnalysis(
#'           analysisId = i,
#'           description = cohortDefinitionSet[i, ]$compositeName,
#'           createEvaluationCohortArgs = cohortArgsAcute,
#'           testPhenotypeAlgorithmArgs = conditionAlgTestArgs
#'         )
#'       pheValuatorAnalysisList[[i]] <- analysis
#'     }
#'
#'     PheValuator::savePheValuatorAnalysisList(
#'       pheValuatorAnalysisList =
#'         pheValuatorAnalysisList,
#'       file =
#'         file.path(outputFolder, "pheValuatorAnalysisSettings.json")
#'     )
#'
#'     pheValuatorAnalysisList <-
#'       PheValuator::loadPheValuatorAnalysisList(file.path(outputFolder, "pheValuatorAnalysisSettings.json"))
#'
#'     return(pheValuatorAnalysisList)
#'   }
#'
#' #'
#' #' @export
#' getPheValuatorAnalysisTable <- function(pheValuatorAnalysisList) {
#'   data <- PheValuator:::createReferenceTable(pheValuatorAnalysisList)
#'   return(data)
#' }
#'
#'
#'
#' #'
#' #' @export
#' executePheValuatorInParallel <- function(cdmSources,
#'                                          phenotype = "no name given",
#'                                          outputFolder,
#'                                          cohortDefinitionSet,
#'                                          cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable = "cohort"),
#'                                          userService = "OHDSI_USER",
#'                                          passwordService = "OHDSI_PASSWORD",
#'                                          databaseIds = getListOfDatabaseIds(),
#'                                          sequence = 1,
#'                                          analysisName = "Main",
#'                                          xSensCohortId = prevalenceCohortId,
#'                                          xSpecCohortId,
#'                                          daysFromxSpec,
#'                                          prevalenceCohortId,
#'                                          excludedCovariateConceptIds = NULL,
#'                                          addDescendantsToExclude = TRUE,
#'                                          startDayWindow1 = 0,
#'                                          endDayWindow1 = 30,
#'                                          startDayWindow2 = 31,
#'                                          endDayWindow2 = 60,
#'                                          startDayWindow3 = 61,
#'                                          endDayWindow3 = 365,
#'                                          xSpecCohortSize = 5000,
#'                                          modelBaseSampleSize = 25000,
#'                                          baseSampleSize = 200000,
#'                                          startDate = "20100101",
#'                                          endDate = "21000101",
#'                                          cutPoints = c("EV"),
#'                                          defaultWashoutPeriod = 0,
#'                                          defaultSplayPrior = 7,
#'                                          defaultSplayPost = 7,
#'                                          falsePositiveNegativeSubjects = 25,
#'                                          maxCores = parallel::detectCores() /
#'                                            3,
#'                                          tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
#'   pheValuatorAnalysisList <-
#'     getPheValuatorAnalysisList(
#'       xSensCohortId = xSensCohortId,
#'       xSpecCohortId = xSpecCohortId,
#'       daysFromxSpec = daysFromxSpec,
#'       prevalenceCohortId = prevalenceCohortId,
#'       excludedCovariateConceptIds = excludedCovariateConceptIds,
#'       addDescendantsToExclude = addDescendantsToExclude,
#'       startDayWindow1 = startDayWindow1,
#'       endDayWindow1 = endDayWindow1,
#'       startDayWindow2 = startDayWindow2,
#'       endDayWindow2 = endDayWindow2,
#'       startDayWindow3 = startDayWindow3,
#'       endDayWindow3 = endDayWindow3,
#'       xSpecCohortSize = xSpecCohortSize,
#'       modelBaseSampleSize = modelBaseSampleSize,
#'       baseSampleSize = baseSampleSize,
#'       startDate = startDate,
#'       endDate = endDate,
#'       falsePositiveNegativeSubjects = falsePositiveNegativeSubjects,
#'       cutPoints = cutPoints,
#'       defaultWashoutPeriod = defaultWashoutPeriod,
#'       defaultSplayPrior = defaultSplayPrior,
#'       defaultSplayPost = defaultSplayPost,
#'       cohortDefinitionSet = cohortDefinitionSet,
#'       outputFolder = outputFolder
#'     )
#'
#'   cdmSources <- cdmSources |>
#'     dplyr::filter(database %in% c(databaseIds)) |>
#'     dplyr::filter(sequence == !!sequence)
#'
#'   x <- list()
#'   for (i in 1:nrow(cdmSources)) {
#'     x[[i]] <- cdmSources[i, ]
#'   }
#'
#'   # use Parallel Logger to run in parallel
#'   cluster <-
#'     ParallelLogger::makeCluster(
#'       numberOfThreads = min(
#'         as.integer(trunc(
#'           parallel::detectCores() /
#'             2
#'         )),
#'         nrow(cdmSources)
#'       ),
#'       maxCores
#'     )
#'
#'   ## file logger
#'   loggerName <-
#'     paste0(
#'       "PV_",
#'       stringr::str_replace_all(
#'         string = Sys.time(),
#'         pattern = ":|-|EDT| ",
#'         replacement = ""
#'       )
#'     )
#'
#'   ParallelLogger::addDefaultFileLogger(fileName = file.path(outputFolder, paste0(loggerName, ".txt")))
#'
#'
#'   executePheValuatorX <- function(x,
#'                                   phenotype,
#'                                   analysisName,
#'                                   cohortTableNames,
#'                                   outputFolder,
#'                                   databaseId,
#'                                   pheValuatorAnalysisList,
#'                                   tempEmulationSchema) {
#'     cohortTableName <- cohortTableNames$cohortTable
#'
#'     connectionDetails <- DatabaseConnector::createConnectionDetails(
#'       dbms = x$dbms,
#'       user = keyring::key_get(userService),
#'       password = keyring::key_get(passwordService),
#'       server = x$serverFinal,
#'       port = x$port
#'     )
#'
#'     pheValuatorOutputFolder <-
#'       file.path(outputFolder, x$sourceKey)
#'
#'     dir.create(
#'       path = pheValuatorOutputFolder,
#'       showWarnings = FALSE,
#'       recursive = TRUE
#'     )
#'
#'     PheValuator::runPheValuatorAnalyses(
#'       connectionDetails = connectionDetails,
#'       phenotype = phenotype,
#'       analysisName = analysisName,
#'       cdmDatabaseSchema = x$cdmDatabaseSchema,
#'       cohortDatabaseSchema = x$cohortDatabaseSchema,
#'       workDatabaseSchema = x$cohortDatabaseSchema,
#'       databaseId = x$sourceKey,
#'       cohortTable = cohortTableName,
#'       outputFolder = pheValuatorOutputFolder,
#'       tempEmulationSchema = tempEmulationSchema,
#'       pheValuatorAnalysisList = pheValuatorAnalysisList
#'     )
#'
#'     if (file.exists(file.path(
#'       pheValuatorOutputFolder,
#'       "exportFolder",
#'       "pv_test_subjects.csv"
#'     ))) {
#'       connection <-
#'         DatabaseConnector::connect(connectionDetails = connectionDetails)
#'
#'       cohortDataToUpload <-
#'         readr::read_csv(
#'           file = file.path(
#'             pheValuatorOutputFolder,
#'             "exportFolder",
#'             "pv_test_subjects.csv"
#'           ),
#'           show_col_types = FALSE
#'         ) |>
#'         SqlRender::snakeCaseToCamelCaseNames() |>
#'         dplyr::select(
#'           subjectId,
#'           cohortStartDate,
#'           type
#'         ) |>
#'         dplyr::mutate(cohortEndDate = cohortStartDate) |>
#'         dplyr::inner_join(
#'           y = dplyr::tibble(
#'             type = c("TP", "FP", "TN", "FN"),
#'             cohortDefinitionId = c(1, 2, 3, 4)
#'           ),
#'           by = "type"
#'         ) |>
#'         dplyr::select(
#'           cohortDefinitionId,
#'           subjectId,
#'           cohortStartDate,
#'           cohortEndDate
#'         )
#'
#'       ParallelLogger::logInfo("Uploading Probabilistic cohorts.")
#'
#'       tempTableName <-
#'         paste0("#t", (as.numeric(as.POSIXlt(Sys.time()))) * 100000)
#'
#'       DatabaseConnector::insertTable(
#'         connection = connection,
#'         tableName = tempTableName,
#'         dropTableIfExists = TRUE,
#'         tempTable = TRUE,
#'         tempEmulationSchema = tempEmulationSchema,
#'         data = cohortDataToUpload,
#'         camelCaseToSnakeCase = TRUE,
#'         bulkLoad = TRUE,
#'         progressBar = TRUE,
#'         createTable = TRUE
#'       )
#'
#'       cohortDefinitionSetWithProablisticCohort <-
#'         dplyr::tibble(
#'           cohortName = paste0(paste0(phenotype, " "), c("TP", "FP", "TN", "FN")),
#'           cohortDefinitionId = c(1, 2, 3, 4)
#'         )
#'
#'       CohortExplorerOutputFolder <- file.path(outputFolder |> dirname(), "CohortExplorer")
#'       for (j in (1:nrow(cohortDefinitionSetWithProablisticCohort))) {
#'         CohortExplorer::createCohortExplorerApp(
#'           connection = connection,
#'           cohortDatabaseSchema = NULL,
#'           cdmDatabaseSchema = x$cdmDatabaseSchema,
#'           vocabularyDatabaseSchema = x$cdmDatabaseSchema,
#'           tempEmulationSchema = tempEmulationSchema,
#'           cohortTable = tempTableName,
#'           cohortDefinitionId = cohortDefinitionSetWithProablisticCohort[j, ]$cohortDefinitionId,
#'           cohortName = cohortDefinitionSetWithProablisticCohort[j, ]$cohortName,
#'           doNotExportCohortData = FALSE,
#'           sampleSize = falsePositiveNegativeSubjects,
#'           personIds = NULL,
#'           exportFolder = file.path(CohortExplorerOutputFolder, x$sourceKey),
#'           databaseId = SqlRender::snakeCaseToCamelCase(x$sourceKey),
#'           shiftDates = FALSE,
#'           assignNewId = FALSE
#'         )
#'       }
#'
#'       DatabaseConnector::renderTranslateExecuteSql(
#'         connection = connection,
#'         sql = "DROP TABLE IF EXISTS @temp_table;",
#'         profile = FALSE,
#'         progressBar = FALSE,
#'         reportOverallTime = FALSE,
#'         tempEmulationSchema = tempEmulationSchema,
#'         temp_table = tempTableName
#'       )
#'
#'       CohortExplorer::exportCohortExplorerAppFiles(file.path(CohortExplorerOutputFolder, "Combined"))
#'
#'       cohortExplorerOutput <-
#'         list.files(
#'           path = CohortExplorerOutputFolder,
#'           pattern = "CohortExplorer_",
#'           all.files = TRUE,
#'           full.names = TRUE,
#'           recursive = TRUE
#'         )
#'
#'       for (i in (1:length(cohortExplorerOutput))) {
#'         unlink(
#'           x = file.path(CohortExplorerOutputFolder, "data", basename(cohortExplorerOutput[[i]])),
#'           recursive = TRUE,
#'           force = TRUE
#'         )
#'         if (cohortExplorerOutput[[i]] !=
#'           file.path(CohortExplorerOutputFolder, "Combined", "data", basename(cohortExplorerOutput[[i]]))) {
#'           file.copy(
#'             from = cohortExplorerOutput[[i]],
#'             to = file.path(CohortExplorerOutputFolder, "Combined", "data", basename(cohortExplorerOutput[[i]])),
#'             overwrite = TRUE
#'           )
#'         }
#'       }
#'     }
#'   }
#'
#'   cohortTableName <- cohortTableNames$cohortTable
#'
#'   ParallelLogger::clusterApply(
#'     cluster = cluster,
#'     x = x,
#'     phenotype = phenotype,
#'     analysisName = analysisName,
#'     cohortTableNames = cohortTableNames,
#'     outputFolder = outputFolder,
#'     tempEmulationSchema = tempEmulationSchema,
#'     pheValuatorAnalysisList = pheValuatorAnalysisList,
#'     fun = executePheValuatorX,
#'     stopOnError = FALSE
#'   )
#'
#'   ParallelLogger::stopCluster(cluster = cluster)
#'
#'   filesWithResults <-
#'     list.files(
#'       path = outputFolder,
#'       pattern = "pv_algorithm_performance_results.csv",
#'       full.names = TRUE,
#'       recursive = TRUE
#'     )
#'
#'   collectOutput <- c()
#'   if (length(filesWithResults) > 0) {
#'     for (i in (1:length(filesWithResults))) {
#'       collectOutput[[i]] <-
#'         readr::read_csv(file = filesWithResults[i], col_types = readr::cols())
#'     }
#'     collectOutput <- dplyr::bind_rows(collectOutput)
#'     collectOutput <-
#'       SqlRender::snakeCaseToCamelCaseNames(collectOutput)
#'
#'     collectOutput <- collectOutput |>
#'       dplyr::left_join(
#'         cohortDefinitionSet |>
#'           dplyr::select(
#'             cohortId,
#'             cohortName
#'           ),
#'         by = "cohortId"
#'       ) |>
#'       dplyr::relocate(
#'         phenotype,
#'         databaseId,
#'         cohortId,
#'         cohortName
#'       )
#'
#'     unlink(
#'       file.path(
#'         outputFolder,
#'         "Combined",
#'         "PheValuator",
#'         "pv_algorithm_performance_results.csv"
#'       ),
#'       recursive = TRUE,
#'       force = TRUE
#'     )
#'
#'     readr::write_excel_csv(
#'       x = collectOutput,
#'       file = file.path(
#'         outputFolder,
#'         "pv_algorithm_performance_results.csv"
#'       ),
#'       na = "",
#'       append = FALSE
#'     )
#'   }
#'   ParallelLogger::clearLoggers()
#'   return(collectOutput)
#' }
