#' @export
performConceptSetDiagnostics <-
  function(outputFolder = "01 ConceptSetDiagnostics",
           concepSetDefinition,
           domainTableName = "drug_exposure",
           cdmSources,
           performTemporalCharcterization = FALSE) {
    # Resolving and Mapping Concept IDs
    # ---------------------------------
    # Obtain resolved and mapped concept IDs from database
    cdmSourceSelected <-
      OhdsiHelpers::getCdmSource(cdmSources = cdmSources)
    connectionDetailsSelected <-
      OhdsiHelpers::createConnectionDetails(cdmSources = cdmSources)
    connection <-
      DatabaseConnector::connect(connectionDetails = connectionDetailsSelected)

    resolveAndMapConcepts <-
      function(conceptSetDefinition,
               connection,
               cdmSource) {
        resolvedConcepts <-
          ConceptSetDiagnostics::resolveConceptSetExpression(
            conceptSetExpression = RJSONIO::fromJSON(conceptSetDefinition$conceptSetDefinition, digits = 23),
            connection = connection,
            vocabularyDatabaseSchema = cdmSource$vocabDatabaseSchema
          ) |>
          dplyr::mutate(conceptSetId = conceptSetDefinition$conceptSetId)

        mappedConcepts <-
          ConceptSetDiagnostics::getMappedSourceConcepts(
            conceptIds = resolvedConcepts$conceptId,
            connection = connection,
            vocabularyDatabaseSchema = cdmSource$vocabDatabaseSchema
          ) |>
          dplyr::mutate(conceptSetId = conceptSetDefinition$conceptSetId)

        list(
          resolvedConcepts = resolvedConcepts,
          mappedConcepts = mappedConcepts
        )
      }

    resolvedAndMappedConcepts <-
      lapply(1:nrow(concepSetDefinition), function(i) {
        resolveAndMapConcepts(concepSetDefinition[i, ], connection, cdmSourceSelected)
      })

    resolvedConcepts <-
      dplyr::bind_rows(lapply(resolvedAndMappedConcepts, `[[`, "resolvedConcepts")) |>
      dplyr::distinct() |>
      dplyr::arrange(conceptSetId, conceptId)
    mappedConcepts <-
      dplyr::bind_rows(lapply(resolvedAndMappedConcepts, `[[`, "mappedConcepts")) |>
      dplyr::distinct() |>
      dplyr::arrange(conceptSetId, givenConceptId, conceptId)

    saveRDS(
      resolvedConcepts,
      file.path(outputFolder, "ResolvedConcepts.RDS")
    )
    saveRDS(
      mappedConcepts,
      file.path(outputFolder, "MappedConcepts.RDS")
    )

    # Fetching and Saving Concept ID Details ----
    conceptIdDetails <- ConceptSetDiagnostics::getConceptIdDetails(
      connection = connection,
      conceptIds = unique(c(
        resolvedConcepts$conceptId, mappedConcepts$conceptId
      )),
      vocabularyDatabaseSchema = cdmSourceSelected$vocabDatabaseSchema
    ) |>
      dplyr::arrange(conceptId)

    saveRDS(
      conceptIdDetails,
      file.path(outputFolder, "ConceptIdDetails.RDS")
    )

    if (performTemporalCharcterization) {
      # Temporal Utilization Diagnostics ----
      for (i in 1:nrow(concepSetDefinition)) {
        outputLocation <-
          file.path(
            outputFolder,
            "ConceptRecordCount",
            concepSetDefinition[i, ]$conceptSetId
          )
        dir.create(
          path = outputLocation,
          showWarnings = FALSE,
          recursive = TRUE
        )

        conceptIds <- c(
          resolvedConcepts |>
            dplyr::filter(conceptSetId %in% concepSetDefinition[i, ]$conceptSetId) |>
            dplyr::pull(conceptId),
          mappedConcepts |>
            dplyr::filter(conceptSetId %in% concepSetDefinition[i, ]$conceptSetId) |>
            dplyr::pull(conceptId)
        ) |>
          unique() |>
          sort()

        OhdsiHelpers::executeConceptRecordCountInParallel(
          cdmSources = cdmSources,
          domainTableName = domainTableName,
          outputFolder = outputLocation,
          conceptIds = conceptIds
        )
      }

      conceptRecordCount <-
        lapply(1:nrow(concepSetDefinition), function(i) {
          outputLocation <-
            file.path(
              outputFolder,
              "ConceptRecordCount",
              concepSetDefinition[i, ]$conceptSetId
            )
          rdsFiles <- list.files(
            path = outputLocation,
            pattern = "ConceptRecordCount.RDS",
            all.files = TRUE,
            full.names = TRUE,
            recursive = TRUE,
            include.dirs = TRUE
          )

          allRds <- lapply(rdsFiles, readRDS)
          bindedRds <- do.call(dplyr::bind_rows, allRds) |>
            dplyr::distinct() |>
            dplyr::mutate(conceptSetId = concepSetDefinition[i, ]$conceptSetId) |>
            dplyr::relocate(conceptSetId)

          return(bindedRds)
        })

      conceptRecordCount |>
        dplyr::bind_rows() |>
        saveRDS(file.path(outputFolder, "ConceptRecordCount.RDS"))
    }
  }
