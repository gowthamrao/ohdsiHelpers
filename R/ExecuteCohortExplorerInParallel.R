#' @export
executeCohortExplorerInParallel <-
  function(cdmSources,
           outputFolder,
           cohortDefinitionSet,
           cohortTableNames = CohortGenerator::getCohortTableNames(cohortTable = "cohort"),
           userService = "OHDSI_USER",
           passwordService = "OHDSI_PASSWORD",
           tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
           databaseIds = getListOfDatabaseIds(),
           sequence = 1,
           cohortIds = NULL) {
    cdmSources <- cdmSources |>
      dplyr::filter(database %in% c(databaseIds)) |>
      dplyr::filter(sequence == !!sequence)
    
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
      length(x)))
    
    ## file logger
    loggerName <-
      paste0(
        "CE_",
        stringr::str_replace_all(
          string = Sys.time(),
          pattern = ":|-|EDT| ",
          replacement = ''
        )
      )
    loggerTrace <-
      ParallelLogger::addDefaultFileLogger(fileName = file.path(outputFolder, paste0(loggerName, ".txt")))
    
    
    executeCohortExplorerX <- function(x,
                                       cohortDefinitionSet,
                                       cohortIds,
                                       cohortTableNames,
                                       outputFolder,
                                       tempEmulationSchema) {
      connectionDetails <- DatabaseConnector::createConnectionDetails(
        dbms = x$dbms,
        user = keyring::key_get(userService),
        password = keyring::key_get(passwordService),
        server = x$serverFinal,
        port = x$port
      )
      outputFolder <-
        file.path(outputFolder, x$sourceKey)
      
      for (i in (1:length(cohortIds))) {
        CohortExplorer::createCohortExplorerApp(
          cohortDefinitionId = cohortIds[[i]],
          cohortName = cohortDefinitionSet |>
            dplyr::filter(cohortId == cohortIds[[i]]) |>
            dplyr::pull(cohortName),
          exportFolder = outputFolder,
          databaseId = SqlRender::snakeCaseToCamelCase(x$sourceKey),
          cohortDatabaseSchema = x$cohortDatabaseSchemaFinal,
          connectionDetails = connectionDetails,
          connection = NULL,
          cdmDatabaseSchema = x$cdmDatabaseSchemaFinal,
          vocabularyDatabaseSchema = x$vocabDatabaseSchemaFinal,
          tempEmulationSchema = tempEmulationSchema,
          cohortTable = cohortTableNames$cohortTable,
          featureCohortDatabaseSchema = x$cohortDatabaseSchemaFinal,
          featureCohortDefinitionSet = cohortDefinitionSet,
          featureCohortTable = cohortTableNames$cohortTable
        )
      }
    }
    
    ParallelLogger::clusterApply(
      cluster = cluster,
      x = x,
      cohortDefinitionSet = cohortDefinitionSet,
      cohortIds = cohortIds,
      outputFolder = outputFolder,
      cohortTableNames = cohortTableNames,
      tempEmulationSchema = tempEmulationSchema,
      fun = executeCohortExplorerX,
      stopOnError = FALSE
    )
    
    ParallelLogger::stopCluster(cluster = cluster)
  }
