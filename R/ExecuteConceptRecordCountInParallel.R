#' @export
executeConceptRecordCountInParallel <-
  function(cdmSources,
           outputFolder,
           conceptIds = NULL,
           userService = "OHDSI_USER",
           passwordService = "OHDSI_PASSWORD",
           tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
           databaseIds = getListOfDatabaseIds(),
           sequence = 1,
           minCellCount = 0,
           domainTableName = NULL) {
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
        "CR_",
        stringr::str_replace_all(
          string = Sys.time(),
          pattern = ":|-|EDT| ",
          replacement = ""
        )
      )

    ParallelLogger::addDefaultFileLogger(fileName = file.path(outputFolder, paste0(loggerName, ".txt")))

    executeConceptRecordCount <- function(x,
                                          conceptIds,
                                          outputFolder,
                                          tempEmulationSchema,
                                          minCellCount,
                                          domainTableName) {
      connectionDetails <- DatabaseConnector::createConnectionDetails(
        dbms = x$dbms,
        user = keyring::key_get(userService),
        password = keyring::key_get(passwordService),
        server = x$serverFinal,
        port = x$port
      )
      outputFolder <-
        file.path(outputFolder, x$sourceKey)

      if (is.null(domainTableName)) {
        domainTableName <- c(
          "drug_exposure",
          "condition_occurrence",
          "procedure_occurrence",
          "mesaurement",
          "observation"
        )
      }

      dir.create(
        path = outputFolder,
        showWarnings = FALSE,
        recursive = TRUE
      )

      output <- ConceptSetDiagnostics::getConceptRecordCount(
        conceptIds = conceptIds,
        connectionDetails = connectionDetails,
        cdmDatabaseSchema = x$cdmDatabaseSchemaFinal,
        vocabularyDatabaseSchema = x$vocabDatabaseSchemaFinal,
        tempEmulationSchema = tempEmulationSchema,
        domainTableName = domainTableName
      ) |>
        dplyr::mutate(
          sourceKey = x$sourceKey,
          database = x$database
        )

      saveRDS(
        object = output,
        file = file.path(outputFolder, "ConceptRecordCount.RDS")
      )
    }

    ParallelLogger::clusterApply(
      cluster = cluster,
      x = x,
      conceptIds = conceptIds,
      outputFolder = outputFolder,
      tempEmulationSchema = tempEmulationSchema,
      fun = executeConceptRecordCount,
      minCellCount = minCellCount,
      domainTableName = domainTableName,
      stopOnError = FALSE
    )

    ParallelLogger::stopCluster(cluster = cluster)
  }
