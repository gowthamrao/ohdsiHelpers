#' Execute the cohort generation
#'
#' @details
#' This function executes the cohort generation
#'
#' @param connectionDetails                   An object of type \code{connectionDetails} as created
#'                                            using the
#'                                            \code{\link[DatabaseConnector]{createConnectionDetails}}
#'                                            function in the DatabaseConnector package.
#' @param cdmDatabaseSchema                   Schema name where your patient-level data in OMOP CDM
#'                                            format resides. Note that for SQL Server, this should
#'                                            include both the database and schema name, for example
#'                                            'cdm_data.dbo'.
#' @param cohortDatabaseSchema                Schema name where intermediate data can be stored. You
#'                                            will need to have write privileges in this schema. Note
#'                                            that for SQL Server, this should include both the
#'                                            database and schema name, for example 'cdm_data.dbo'.
#' @param vocabularyDatabaseSchema            Schema name where your OMOP vocabulary data resides. This
#'                                            is commonly the same as cdmDatabaseSchema. Note that for
#'                                            SQL Server, this should include both the database and
#'                                            schema name, for example 'vocabulary.dbo'.
#' @param cohortTableNames                    cohortTableNames
#' @param tempEmulationSchema                 Some database platforms like Oracle and Impala do not
#'                                            truly support temp tables. To emulate temp tables,
#'                                            provide a schema with write privileges where temp tables
#'                                            can be created.
#' @param outputFolder                        Name of local folder to place results; make sure to use
#'                                            forward slashes (/). Do not use a folder on a network
#'                                            drive since this greatly impacts performance.
#' @param createCohortTableIncremental         When set to TRUE, this function will check to see if the
#'                                            cohortTableNames exists in the cohortDatabaseSchema and if
#'                                            they exist, it will skip creating the tables.
#' @param generateCohortIncremental           Create only cohorts that haven't been created before?
#' @param incrementalFolder                   Name of local folder to hold the logs for incremental
#'                                            run; make sure to use forward slashes (/). Do not use a
#'                                            folder on a network drive since this greatly impacts
#'                                            performance.
#' @param cohortDefinitionSet                 Cohort Definition set object
#' @param databaseId                          database id
#' @param cohortIds                           Do you want to limit the execution to only some cohort ids.
#'
#' @export
executeCohortGeneration <- function(connectionDetails,
                                    cohortDefinitionSet,
                                    cdmDatabaseSchema,
                                    vocabularyDatabaseSchema = cdmDatabaseSchema,
                                    cohortDatabaseSchema = cdmDatabaseSchema,
                                    cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable = "cohort"),
                                    tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                    outputFolder,
                                    databaseId,
                                    createCohortTableIncremental = TRUE,
                                    generateCohortIncremental = TRUE,
                                    incrementalFolder = file.path(outputFolder, "incrementalFolder"),
                                    cohortIds = NULL) {
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

  ParallelLogger::logInfo("Creating cohorts")

  # Next create the tables on the database
  CohortGenerator::createCohortTables(
    connectionDetails = connectionDetails,
    cohortTableNames = cohortTableNames,
    cohortDatabaseSchema = cohortDatabaseSchema,
    incremental = createCohortTableIncremental
  )

  if (!is.null(cohortIds)) {
    cohortDefinitionSet <- cohortDefinitionSet |>
      dplyr::filter(.data$cohortId %in% c(cohortIds))
  }

  # Generate the cohort set
  CohortGenerator::generateCohortSet(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortDefinitionSet,
    tempEmulationSchema = tempEmulationSchema,
    incrementalFolder = incrementalFolder,
    stopOnError = FALSE,
    incremental = generateCohortIncremental
  )

  # export stats table to local
  CohortGenerator::exportCohortStatsTables(
    connectionDetails = connectionDetails,
    connection = NULL,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortStatisticsFolder = outputFolder,
    incremental = generateCohortIncremental
  )

  cohortStatsTables <- getCohortStatsFix(
    connectionDetails = connectionDetails,
    connection = NULL,
    databaseId = databaseId,
    cohortDatabaseSchema = cohortDatabaseSchema,
    snakeCaseToCamelCase = TRUE,
    cohortTableNames = cohortTableNames,
  )

  cohortCount <- CohortGenerator::getCohortCounts(
    connectionDetails = connectionDetails,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortTable = cohortTableNames$cohortTable,
    cohortDefinitionSet = NULL,
    cohortIds = cohortDefinitionSet$cohortId,
    databaseId = databaseId
  )
  readr::write_excel_csv(
    x = cohortCount |>
      dplyr::select(
        .data$cohortId,
        .data$cohortEntries,
        .data$cohortSubjects
      ) |>
      dplyr::arrange(.data$cohortId),
    file = file.path(
      outputFolder,
      "cohortCount.csv"
    ),
    na = "",
    append = FALSE,
    progress = FALSE
  )

  output <- c()
  output$cohortCount <- cohortCount
  output$cohortInclusionTable <- cohortStatsTables$cohortInclusionTable
  output$cohortInclusionResultTable <- cohortStatsTables$cohortInclusionResultTable
  output$cohortInclusionStatsTable <- cohortStatsTables$cohortInclusionStatsTable
  output$cohortSummaryStatsTable <- cohortStatsTables$cohortSummaryStatsTable
  output$cohortSummaryStatsTable <- cohortStatsTables$cohortSummaryStatsTable

  saveRDS(
    object = output,
    file = file.path(
      outputFolder,
      "CohortGenerator.RDS"
    )
  )
  return(output)
}


#' @export
executeCohortGenerationInParallel <- function(cdmSources,
                                              outputFolder,
                                              cohortDefinitionSet,
                                              cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable = "cohort"),
                                              userService = "OHDSI_USER",
                                              passwordService = "OHDSI_PASSWORD",
                                              tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                              databaseIds = getListOfDatabaseIds(),
                                              sequence = 1,
                                              createCohortTableIncremental = TRUE,
                                              generateCohortIncremental = TRUE,
                                              cohortIds = NULL) {
  cdmSources <- cdmSources |>
    dplyr::filter(.data$database %in% c(databaseIds)) |>
    dplyr::filter(.data$sequence == !!sequence)

  x <- list()
  for (i in 1:nrow(cdmSources)) {
    x[[i]] <- cdmSources[i, ]
  }

  # use Parallel Logger to run in parallel
  cluster <-
    ParallelLogger::makeCluster(numberOfThreads = min(
      as.integer(trunc(
        parallel::detectCores() /
          2
      )),
      length(x)
    ))

  ## file logger
  loggerName <-
    paste0(
      "CG_",
      stringr::str_replace_all(
        string = Sys.time(),
        pattern = ":|-|EDT| ",
        replacement = ""
      )
    )
  
  ParallelLogger::addDefaultFileLogger(fileName = file.path(outputFolder, paste0(loggerName, ".txt")))

  executeCohortGenerationX <- function(x,
                                       cohortDefinitionSet,
                                       cohortIds,
                                       cohortTableNames,
                                       outputFolder,
                                       tempEmulationSchema,
                                       createCohortTableIncremental,
                                       generateCohortIncremental) {
    connectionDetails <- DatabaseConnector::createConnectionDetails(
      dbms = x$dbms,
      user = keyring::key_get(userService),
      password = keyring::key_get(passwordService),
      server = x$serverFinal,
      port = x$port
    )

    executeCohortGeneration(
      connectionDetails = connectionDetails,
      cohortDefinitionSet = cohortDefinitionSet,
      cdmDatabaseSchema = x$cdmDatabaseSchemaFinal,
      vocabularyDatabaseSchema = x$vocabDatabaseSchemaFinal,
      cohortDatabaseSchema = x$cohortDatabaseSchemaFinal,
      cohortTableNames = cohortTableNames,
      databaseId = x$sourceKey,
      tempEmulationSchema = tempEmulationSchema,
      outputFolder = file.path(outputFolder, x$sourceKey),
      cohortIds = cohortIds,
      createCohortTableIncremental = createCohortTableIncremental,
      generateCohortIncremental = generateCohortIncremental
    )
  }

  ParallelLogger::clusterApply(
    cluster = cluster,
    x = x,
    cohortDefinitionSet = cohortDefinitionSet,
    cohortIds = cohortIds,
    outputFolder = outputFolder,
    cohortTableNames = cohortTableNames,
    tempEmulationSchema = tempEmulationSchema,
    createCohortTableIncremental = createCohortTableIncremental,
    generateCohortIncremental = generateCohortIncremental,
    fun = executeCohortGenerationX
  )

  ParallelLogger::stopCluster(cluster = cluster)
  ParallelLogger::clearLoggers()
}
