getDefaultCovariateSettings <- function() {
  FeatureExtraction::createTemporalCovariateSettings(
    useDemographicsGender = TRUE,
    useDemographicsAge = TRUE,
    useDemographicsAgeGroup = TRUE,
    useDemographicsRace = TRUE,
    useDemographicsEthnicity = TRUE,
    useDemographicsIndexYear = TRUE,
    useDemographicsIndexMonth = TRUE,
    useDemographicsIndexYearMonth = TRUE,
    useDemographicsPriorObservationTime = TRUE,
    useDemographicsPostObservationTime = TRUE,
    useDemographicsTimeInCohort = TRUE,
    useConditionOccurrence = TRUE,
    useProcedureOccurrence = TRUE,
    useDrugEraStart = TRUE,
    useMeasurement = TRUE,
    useConditionEraStart = TRUE,
    useConditionEraOverlap = TRUE,
    useVisitCount = TRUE,
    useVisitConceptCount = TRUE,
    useConditionEraGroupStart = FALSE, # do not use because https://github.com/OHDSI/FeatureExtraction/issues/144
    useConditionEraGroupOverlap = TRUE,
    useDrugExposure = FALSE, # leads to too many concept id
    useDrugEraOverlap = FALSE,
    useDrugEraGroupStart = FALSE, # do not use because https://github.com/OHDSI/FeatureExtraction/issues/144
    useDrugEraGroupOverlap = TRUE,
    useObservation = TRUE,
    useDeviceExposure = TRUE,
    useCharlsonIndex = TRUE,
    useDcsi = TRUE,
    useChads2 = TRUE,
    useChads2Vasc = TRUE,
    useHfrs = FALSE,
    temporalStartDays = c(
      # components displayed in cohort characterization
      -9999, # anytime prior
      -365, # long term prior
      -180, # medium term prior
      -30, # short term prior

      # components displayed in temporal characterization
      -365, # one year prior to -31
      -30, # 30 day prior not including day 0
      0, # index date only
      1, # 1 day after to day 30
      31,
      -9999 # Any time prior to any time future
    ),
    temporalEndDays = c(
      0, # anytime prior
      0, # long term prior
      0, # medium term prior
      0, # short term prior

      # components displayed in temporal characterization
      -31, # one year prior to -31
      -1, # 30 day prior not including day 0
      0, # index date only
      30, # 1 day after to day 30
      365,
      9999 # Any time prior to any time future
    )
  )
}



#' @export
executeCohortDiagnosticsInParallel <-
  function(cdmSources,
           outputFolder,
           cohortDefinitionSet,
           cohortTableNames = NULL,
           usePhenotypeLibrarySettings = FALSE,
           userService = "OHDSI_USER",
           passwordService = "OHDSI_PASSWORD",
           tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
           databaseIds = getListOfDatabaseIds(),
           sequence = 1,
           cohortIds = NULL,
           runInclusionStatistics = TRUE,
           runIncludedSourceConcepts = TRUE,
           runOrphanConcepts = TRUE,
           runTimeSeries = FALSE,
           runVisitContext = TRUE,
           runBreakdownIndexEvents = TRUE,
           runIncidenceRate = TRUE,
           runCohortRelationship = TRUE,
           featureCohortTableName = NULL,
           featureCohortDefinitionSet = cohortDefinitionSet,
           runTemporalCohortCharacterization = TRUE,
           temporalCovariateSettings = getDefaultCovariateSettings(),
           useSubsetCohortsAsFeatures = FALSE,
           createMergedFile = FALSE,
           minCellCount = 5) {
    cdmSources <- cdmSources |>
      dplyr::filter(.data$database %in% c(databaseIds)) |>
      dplyr::filter(.data$sequence == !!sequence)

    if (nrow(cdmSources) == 0) {
      stop("no matching cdm sources.")
    }

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
        "CD_",
        stringr::str_replace_all(
          string = Sys.time(),
          pattern = ":|-|EDT| ",
          replacement = ""
        )
      )

    ParallelLogger::addDefaultFileLogger(fileName = file.path(outputFolder, paste0(loggerName, ".txt")))

    executeCohortDiagnosticsX <- function(x,
                                          cohortDefinitionSet,
                                          cohortIds,
                                          cohortTableNames,
                                          usePhenotypeLibrarySettings,
                                          outputFolder,
                                          tempEmulationSchema,
                                          runInclusionStatistics,
                                          runIncludedSourceConcepts,
                                          runOrphanConcepts,
                                          runTimeSeries,
                                          runVisitContext,
                                          runBreakdownIndexEvents,
                                          runIncidenceRate,
                                          runCohortRelationship,
                                          runTemporalCohortCharacterization,
                                          temporalCovariateSettings,
                                          useSubsetCohortsAsFeatures,
                                          featureCohortTableName,
                                          featureCohortDefinitionSet,
                                          minCellCount) {
      connectionDetails <- DatabaseConnector::createConnectionDetails(
        dbms = x$dbms,
        user = keyring::key_get(userService),
        password = keyring::key_get(passwordService),
        server = x$serverFinal,
        port = x$port
      )
      outputFolder <-
        file.path(outputFolder, x$sourceKey)

      featureExtractionSettingsCohortDiagnostics <- temporalCovariateSettings

      finalFeatureCohortDefinitionSet <- featureCohortDefinitionSet

      if (is.null(cohortTableNames)) {
        if (usePhenotypeLibrarySettings) {
          cohortTableName <- paste0(
            stringr::str_squish("pl_"),
            stringr::str_squish(x$sourceKey)
          )
          cohortTableNames <-
            CohortGenerator::getCohortTableNames(cohortTable = cohortTableName)

          if (is.null(featureCohortTableName)) {
            featureCohortTableName <- cohortTableName
          }
        }
      }

      if (!useSubsetCohortsAsFeatures) {
        if ("isSubset" %in% colnames(finalFeatureCohortDefinitionSet)) {
          finalFeatureCohortDefinitionSet <- finalFeatureCohortDefinitionSet |>
            dplyr::filter(!.data$isSubset == TRUE)
        }
      }

      if (exists("featureExtractionSettingsCohortBasedCovariateSettings1")) {
        featureExtractionSettingsCohortBasedCovariateSettings1 <- NULL
        remove(featureExtractionSettingsCohortBasedCovariateSettings1)
      }
      if (exists("featureExtractionSettingsCohortBasedCovariateSettings2")) {
        featureExtractionSettingsCohortBasedCovariateSettings2 <- NULL
        remove("featureExtractionSettingsCohortBasedCovariateSettings2")
      }
      if (exists("featureExtractionCovariateSettings")) {
        featureExtractionCovariateSettings <- NULL
        remove("featureExtractionCovariateSettings")
      }

      finalFeatureCohorts <- finalFeatureCohortDefinitionSet |>
        dplyr::select(
          .data$cohortId,
          .data$cohortName
        ) |>
        dplyr::filter(!.data$cohortId %in% c(cohortDefinitionSet$cohortId))

      if (nrow(finalFeatureCohorts) > 0) {
        featureExtractionSettingsCohortBasedCovariateSettings1 <-
          FeatureExtraction::createCohortBasedTemporalCovariateSettings(
            analysisId = 150,
            covariateCohortDatabaseSchema = x$cohortDatabaseSchemaFinal,
            covariateCohortTable = featureCohortTableName,
            covariateCohorts = finalFeatureCohorts,
            valueType = "binary",
            temporalStartDays = featureExtractionSettingsCohortDiagnostics$temporalStartDays,
            temporalEndDays = featureExtractionSettingsCohortDiagnostics$temporalEndDays
          )
      }

      featureExtractionSettingsCohortBasedCovariateSettings2 <-
        FeatureExtraction::createCohortBasedTemporalCovariateSettings(
          analysisId = 160,
          covariateCohortDatabaseSchema = x$cohortDatabaseSchemaFinal,
          covariateCohortTable = cohortTableNames$cohortTable,
          covariateCohorts = cohortDefinitionSet |>
            dplyr::select(
              .data$cohortId,
              .data$cohortName
            ),
          valueType = "binary",
          temporalStartDays = featureExtractionSettingsCohortDiagnostics$temporalStartDays,
          temporalEndDays = featureExtractionSettingsCohortDiagnostics$temporalEndDays
        )

      if (exists("featureExtractionSettingsCohortBasedCovariateSettings1")) {
        featureExtractionCovariateSettings <-
          list(
            featureExtractionSettingsCohortDiagnostics,
            featureExtractionSettingsCohortBasedCovariateSettings1,
            featureExtractionSettingsCohortBasedCovariateSettings2
          )
      } else {
        featureExtractionCovariateSettings <-
          list(
            featureExtractionSettingsCohortDiagnostics,
            featureExtractionSettingsCohortBasedCovariateSettings2
          )
      }

      CohortDiagnostics::executeDiagnostics(
        cohortDefinitionSet = cohortDefinitionSet,
        exportFolder = outputFolder,
        databaseId = x$sourceKey,
        databaseName = x$databaseName,
        databaseDescription = x$databaseDescription,
        cohortDatabaseSchema = x$cohortDatabaseSchemaFinal,
        connectionDetails = connectionDetails,
        connection = NULL,
        cdmDatabaseSchema = x$cdmDatabaseSchemaFinal,
        tempEmulationSchema = tempEmulationSchema,
        cohortTable = NULL,
        cohortTableNames = cohortTableNames,
        vocabularyDatabaseSchema = x$vocabDatabaseSchemaFinal,
        cohortIds = cohortIds,
        cdmVersion = 5,
        runInclusionStatistics = runInclusionStatistics,
        runIncludedSourceConcepts = runIncludedSourceConcepts,
        runOrphanConcepts = runOrphanConcepts,
        runTimeSeries = runTimeSeries,
        runVisitContext = runVisitContext,
        runBreakdownIndexEvents = runBreakdownIndexEvents,
        runIncidenceRate = runIncidenceRate,
        runCohortRelationship = runCohortRelationship,
        runTemporalCohortCharacterization = runTemporalCohortCharacterization,
        temporalCovariateSettings = featureExtractionCovariateSettings,
        minCellCount = minCellCount,
        incremental = TRUE,
        incrementalFolder = file.path(outputFolder, "incrementalFolder")
      )
    }

    ParallelLogger::clusterApply(
      cluster = cluster,
      x = x,
      cohortDefinitionSet = cohortDefinitionSet,
      cohortIds = cohortIds,
      outputFolder = outputFolder,
      cohortTableNames = cohortTableNames,
      usePhenotypeLibrarySettings = usePhenotypeLibrarySettings,
      tempEmulationSchema = tempEmulationSchema,
      fun = executeCohortDiagnosticsX,
      runInclusionStatistics = runInclusionStatistics,
      runIncludedSourceConcepts = runIncludedSourceConcepts,
      runOrphanConcepts = runOrphanConcepts,
      runTimeSeries = runTimeSeries,
      runVisitContext = runVisitContext,
      runBreakdownIndexEvents = runBreakdownIndexEvents,
      runIncidenceRate = runIncidenceRate,
      runCohortRelationship = runCohortRelationship,
      runTemporalCohortCharacterization = runTemporalCohortCharacterization,
      temporalCovariateSettings = temporalCovariateSettings,
      useSubsetCohortsAsFeatures = useSubsetCohortsAsFeatures,
      featureCohortTableName = featureCohortTableName,
      featureCohortDefinitionSet = featureCohortDefinitionSet,
      minCellCount = minCellCount,
      stopOnError = FALSE
    )

    ParallelLogger::stopCluster(cluster = cluster)

    if (createMergedFile) {
      dir.create(
        path = file.path(
          outputFolder,
          "Combined"
        ),
        showWarnings = FALSE,
        recursive = TRUE
      )

      # create sqlite db merged file
      CohortDiagnostics::createMergedResultsFile(
        dataFolder = outputFolder,
        overwrite = TRUE,
        sqliteDbPath = file.path(
          outputFolder,
          "Combined",
          "MergedCohortDiagnosticsData.sqlite"
        )
      )

      CohortDiagnostics::createDiagnosticsExplorerZip(
        outputZipfile = file.path(
          outputFolder,
          "Combined",
          "DiagnosticsExplorer.zip"
        ),
        sqliteDbPath = file.path(
          outputFolder,
          "Combined",
          "MergedCohortDiagnosticsData.sqlite"
        ),
        overwrite = TRUE
      )

      unlink(
        x = file.path(
          outputFolder,
          "Combined",
          "MergedCohortDiagnosticsData.sqlite"
        ),
        recursive = TRUE,
        force = TRUE
      )

      zip::unzip(
        zipfile = file.path(
          outputFolder,
          "Combined",
          "DiagnosticsExplorer.zip"
        ),
        overwrite = TRUE,
        junkpaths = FALSE,
        exdir = file.path(
          outputFolder,
          "Combined"
        )
      )
    }

    ParallelLogger::clearLoggers()
  }
