#' Execute the cohort covariate
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
#' @param targetCohortDatabaseSchema          Schema with instantiated target cohorts.
#' @param targetCohortIds                     Target Cohort ids.
#' @param vocabularyDatabaseSchema            Schema name where your OMOP vocabulary data resides. This
#'                                            is commonly the same as cdmDatabaseSchema. Note that for
#'                                            SQL Server, this should include both the database and
#'                                            schema name, for example 'vocabulary.dbo'.
#' @param targetCohortTableName               target Cohort Table Names
#' @param tempEmulationSchema                 Some database platforms like Oracle and Impala do not
#'                                            truly support temp tables. To emulate temp tables,
#'                                            provide a schema with write privileges where temp tables
#'                                            can be created.
#' @param outputFolder                        Name of local folder to place results; make sure to use
#'                                            forward slashes (/). Do not use a folder on a network
#'                                            drive since this greatly impacts performance.
#' @param minCellCount                        Default 5
#' @param minCharacterizationMean             Default 0.00001
#' @param databaseId                          The id of the database
#' @param notes                               Do you want to add any text notes to metadata output
#' @param temporalCovariateSettings           FeatureExtraction temporalCovariateSettings object
#'
#' @export
executeConceptCovariateCharacterization <-
  function(connectionDetails,
           cdmDatabaseSchema,
           vocabularyDatabaseSchema = cdmDatabaseSchema,
           targetCohortDatabaseSchema,
           targetCohortIds,
           targetCohortTableName,
           temporalCovariateSettings = CohortDiagnostics:::getDefaultCovariateSettings(),
           tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
           databaseId = NULL,
           minCellCount = 5,
           minCharacterizationMean = 0.00001,
           notes = NULL,
           outputFolder) {
    if (!file.exists(outputFolder)) {
      dir.create(outputFolder, recursive = TRUE)
    }

    metaData <- dplyr::tibble(
      startDate = Sys.Date() |> as.character(),
      startTime = Sys.time() |> as.character(),
      databaseId = NA,
      notes = NA,
      minCellCount = !!minCellCount
    )

    if (!is.null(databaseId)) {
      metaData <- metaData |>
        dplyr::mutate(databaseId = !!databaseId)
    }
    if (!is.null(notes)) {
      metaData <- metaData |>
        dplyr::mutate(notes = !!notes)
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
    featureExtractionOutput <-
      CohortDiagnostics:::getCohortCharacteristics(
        connection = connection,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortDatabaseSchema = targetCohortDatabaseSchema,
        cohortTable = targetCohortTableName,
        cohortIds = targetCohortIds,
        covariateSettings = temporalCovariateSettings,
        exportFolder = outputFolder
      )

    featureExtractionOutputFinal <- c()
    for (i in (1:length(names(featureExtractionOutput)))) {
      name <- names(featureExtractionOutput)[[i]]
      featureExtractionOutputFinal[[name]] <-
        featureExtractionOutput[[name]] |>
        dplyr::collect()
      if (name == "covariates") {
        featureExtractionOutputFinal[[name]] <-
          featureExtractionOutputFinal[[name]] |>
          dplyr::collect() |>
          dplyr::filter(.data$sumValue > minCellCount) |>
          dplyr::filter(.data$mean >= minCharacterizationMean)
      }
      if (name == "covariatesContinuous") {
        featureExtractionOutputFinal[[name]] <-
          featureExtractionOutputFinal[[name]] |>
          dplyr::collect() |>
          dplyr::filter(.data$countValue > minCellCount) |>
          dplyr::filter(.data$mean >= minCharacterizationMean)
      }
    }

    saveRDS(
      object = featureExtractionOutputFinal,
      file = file.path(outputFolder, "FeatureExtraction.RDS")
    )

    DatabaseConnector::disconnect(connection = connection)

    return(featureExtractionOutputFinal)
  }


#'
#' @export
executeConceptCovariateCharacterizationInParallel <-
  function(cdmSources,
           targetCohortIds,
           targetCohortTableName,
           userService = "OHDSI_USER",
           passwordService = "OHDSI_PASSWORD",
           databaseIds = getListOfDatabaseIds(),
           temporalCovariateSettings = CohortDiagnostics::getDefaultCovariateSettings(),
           sequence = 1,
           maxCores = parallel::detectCores() /
             3,
           minCellCount = 5,
           minCharacterizationMean = 0.00001,
           databaseId = NULL,
           notes = NULL,
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
      ParallelLogger::makeCluster(
        numberOfThreads = min(
          as.integer(trunc(
            parallel::detectCores() /
              2
          )),
          nrow(cdmSources)
        ),
        maxCores
      )

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

    executeCohortCovariateCharacterizationX <-
      function(x,
               targetCohortIds,
               targetCohortTableName,
               userService,
               passwordService,
               databaseIds,
               tempEmulationSchema,
               temporalCovariateSettings,
               minCellCount,
               minCharacterizationMean,
               notes,
               outputFolder) {
        connectionDetails <- DatabaseConnector::createConnectionDetails(
          dbms = x$dbms,
          user = keyring::key_get(userService),
          password = keyring::key_get(passwordService),
          server = x$serverFinal,
          port = x$port
        )

        conceptCovariateCharacterizationOutputFolder <-
          file.path(outputFolder, x$sourceKey)

        dir.create(
          path = conceptCovariateCharacterizationOutputFolder,
          showWarnings = FALSE,
          recursive = TRUE
        )

        featureExtractionOutput <-
          executeConceptCovariateCharacterization(
            connectionDetails = connectionDetails,
            cdmDatabaseSchema = x$cdmDatabaseSchemaFinal,
            targetCohortDatabaseSchema = x$cohortDatabaseSchemaFinal,
            targetCohortIds = targetCohortIds,
            targetCohortTableName = targetCohortTableName,
            temporalCovariateSettings = temporalCovariateSettings,
            minCellCount = minCellCount,
            minCharacterizationMean = minCharacterizationMean,
            notes = notes,
            databaseId = x$sourceKey,
            outputFolder = conceptCovariateCharacterizationOutputFolder
          )
        rm("featureExtractionOutput")
      }

    ParallelLogger::clusterApply(
      cluster = cluster,
      x = x,
      fun = executeCohortCovariateCharacterizationX,
      targetCohortIds = targetCohortIds,
      targetCohortTableName = targetCohortTableName,
      userService = userService,
      passwordService = passwordService,
      databaseIds = databaseIds,
      temporalCovariateSettings = temporalCovariateSettings,
      tempEmulationSchema = tempEmulationSchema,
      outputFolder = outputFolder,
      notes = notes,
      minCellCount = minCellCount,
      minCharacterizationMean = minCharacterizationMean
    )

    ParallelLogger::stopCluster(cluster = cluster)
  }
