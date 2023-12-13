#' @export
performConceptSetDiagnostics <-
  function(outputFolder = "01 ConceptSetDiagnostics",
           concepSetDefinition,
           domainTableName = "drug_exposure",
           cdmSources) {
    # Setup Environment ----
    dir.create(path = outputFolder,
               showWarnings = FALSE,
               recursive = TRUE)
    
    saveRDS(object = conceptSetDefinitions,
            file = file.path(outputFolder, "ConceptSetDefinitions.RDS"))
    readr::write_excel_csv(x = conceptSetDefinitions,
                           file = file.path(outputFolder, "ConceptSetDefinitions.csv"))
    conceptSetDefinitions <-
      readRDS(file.path(outputFolder, "ConceptSetDefinitions.RDS"))
    
    
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
            vocabularyDatabaseSchema = cdmSource$vocabDatabaseSchemaFinal
          ) |>
          dplyr::mutate(conceptSetId = conceptSetDefinition$conceptSetId)
        
        mappedConcepts <-
          ConceptSetDiagnostics::getMappedSourceConcepts(
            conceptIds = resolvedConcepts$conceptId,
            connection = connection,
            vocabularyDatabaseSchema = cdmSource$vocabDatabaseSchemaFinal
          ) |>
          dplyr::mutate(conceptSetId = conceptSetDefinition$conceptSetId)
        
        list(resolvedConcepts = resolvedConcepts,
             mappedConcepts = mappedConcepts)
      }
    
    resolvedAndMappedConcepts <-
      lapply(1:nrow(conceptSetDefinitions), function(i) {
        resolveAndMapConcepts(conceptSetDefinitions[i, ], connection, cdmSourceSelected)
      })
    
    resolvedConcepts <-
      dplyr::bind_rows(lapply(resolvedAndMappedConcepts, `[[`, "resolvedConceptIds")) |>
      dplyr::distinct() |>
      dplyr::arrange(conceptSetId, conceptId)
    mappedConcepts <-
      dplyr::bind_rows(lapply(resolvedAndMappedConcepts, `[[`, "mappedConcepts")) |>
      dplyr::distinct() |>
      dplyr::arrange(conceptSetId, givenConceptId, conceptId)
    
    saveRDS(resolvedConcepts,
            file.path(outputFolder, "ResolvedConcepts.RDS"))
    saveRDS(mappedConcepts,
            file.path(outputFolder, "MappedConcepts.RDS"))
    
    # Fetching and Saving Concept ID Details ----
    conceptIdDetails <- ConceptSetDiagnostics::getConceptIdDetails(
      connection = connection,
      conceptIds = unique(c(
        resolvedConcepts$conceptId, mappedConcepts$conceptId
      )),
      vocabularyDatabaseSchema = cdmSourceSelected$vocabDatabaseSchemaFinal
    ) |>
      dplyr::arrange(conceptId)
    
    saveRDS(conceptIdDetails,
            file.path(outputFolder, "ConceptIdDetails.RDS"))
    
    # Temporal Utilization Diagnostics ----
    for (i in 1:nrow(conceptSetDefinitions)) {
      outputLocation <-
        file.path(outputFolder,
                  "TemporalUtilization",
                  conceptSetDefinitions[i,]$conceptSetId)
      dir.create(path = outputLocation,
                 showWarnings = FALSE,
                 recursive = TRUE)
      OhdsiHelpers::executeConceptRecordCountInParallel(
        cdmSources = cdmSources,
        domainTableName = domainTableName,
        outputFolder = outputLocation,
        conceptIds = conceptIdDetails$conceptId
      )
    }
    
    temporalRecordCount <-
      lapply(1:nrow(conceptSetDefinitions), function(i) {
        outputLocation <-
          file.path(outputFolder,
                    "TemporalUtilization",
                    conceptSetDefinitions[i,]$conceptSetId)
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
          dplyr::mutate(conceptSetId = conceptSetDefinitions[i,]$conceptSetId) |>
          dplyr::relocate(conceptSetId)
        
        return(bindedRds)
      })
    
    temporalRecordCount |>
      dplyr::bind_rows() |>
      saveRDS(file.path(outputFolder, "TemporalUtilization.RDS"))
    
  }