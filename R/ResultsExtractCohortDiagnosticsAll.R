#' @export
resultsExtractConnection <- function(connectionDetails) {
  ResultModelManager::PooledConnectionHandler$new(connectionDetails)
}

#' @export
resultsExtractCohortDiagnosticsDataSource <- function(connection,
                                                      resultsSchema) {
  ##cd data source----
  cohortDiagnosticsDataSource <-
    OhdsiShinyModules::createCdDatabaseDataSource(
      connectionHandler = connection,
      resultDatabaseSettings = ShinyAppBuilder::createDefaultResultDatabaseSettings(schema = resultsSchema)
    )
  
  # Define the list of elements to be converted
  elementsToConvert <-
    c(
      "dbTable",
      "dataModelSpecifications",
      "cohortTable",
      "cohortCountTable",
      "conceptSets",
      "temporalAnalysisRef",
      "temporalCharacterizationTimeIdChoices",
      "characterizationTimeIdChoices"
    )
  
  # Use lapply to convert each element
  cohortDiagnosticsDataSource[elementsToConvert] <-
    lapply(cohortDiagnosticsDataSource[elementsToConvert], dplyr::tibble)
  
  return(cohortDiagnosticsDataSource)
}

#' @export
resultsExtractCohortDiagnosticsIncidenceRate <-
  function(dataSource,
           cohortIds,
           databaseIds,
           stratifyByGender = TRUE,
           stratifyByCalendarYear = TRUE,
           stratifyByAgeGroup = TRUE,
           minPersonYears = 1000,
           minSubjectCount = 0,
           minIncidenceRate = 0) {
    ## cd incidence rate result-----
    cohortDiagnosticsIncidenceRateResult <-
      OhdsiShinyModules:::getIncidenceRateResult(
        dataSource = dataSource,
        cohortIds = cohortIds,
        databaseIds = databaseIds,
        stratifyByGender = stratifyByGender,
        stratifyByAgeGroup = stratifyByAgeGroup,
        stratifyByCalendarYear = stratifyByCalendarYear,
        minPersonYears = minPersonYears,
        minSubjectCount = minSubjectCount
      )
    
    if (all(!is.null(minIncidenceRate),
            minIncidenceRate > 0)) {
      cohortDiagnosticsIncidenceRateResult <-
        cohortDiagnosticsIncidenceRateResult |>
        dplyr::filter(incidenceRate > minIncidenceRate)
    }
    return(cohortDiagnosticsIncidenceRateResult)
  }

#' @export
resultsExtractCohortDiagnosticsCharacterization <-
  function(dataSource,
           cohortIds,
           databaseIds) {
    ## cd characterization result-----
    cohortDiagnosticsCharacterizationResult <-
      OhdsiShinyModules:::queryResultCovariateValue(dataSource = dataSource,
                                                    cohortIds = cohortIds,
                                                    databaseIds = databaseIds)
    
    return(cohortDiagnosticsCharacterizationResult)
  }


#' @export
resultsExtractCohortDiagnosticsConceptsInCohort <-
  function(dataSource,
           cohortIds,
           databaseIds) {
    ## concepts in data----
    cohortDiagnosticsConceptsInCohort <- c()
    for (i in (1:length(cohortIds))) {
      cohortDiagnosticsConceptsInCohort[[i]] <-
        OhdsiShinyModules:::getConceptsInCohort(
          dataSource = dataSource,
          cohortId = cohortIds[[i]],
          databaseIds = databaseIds
        )
    }
    cohortDiagnosticsConceptsInCohort <-
      dplyr::bind_rows(cohortDiagnosticsConceptsInCohort)
    
    return(cohortDiagnosticsConceptsInCohort)
  }

#' @export
resultsExtractCohortDiagnosticsIndexEventBreakdown <-
  function(dataSource,
           cohortIds,
           databaseIds) {
    ## cd index event breakdown
    cohortDiagnosticsIndexEventBreakdown <-
      OhdsiShinyModules:::getIndexEventBreakdown(
        dataSource = dataSource,
        cohortIds = cohortIds,
        databaseIds = databaseIds,
        cohortCount = dataSource$cohortCountTable
      )
    
    return(cohortDiagnosticsIndexEventBreakdown)
  }




#' @export
resultsExtractCohortDiagnosticsAll <- function(connection = NULL,
                                               connectionDetails = NULL,
                                               cohortIds = NULL,
                                               databaseIds = NULL,
                                               resultsSchema,
                                               outputFolder = NULL) {
  if (is.null(connection)) {
    connection <-
      resultsExtractConnection(connectionDetails)
  }
  dataSource <-
    resultsExtractCohortDiagnosticsDataSource(connection = connection,
                                              resultsSchema = resultsSchema)
  
  if (is.null(cohortIds)) {
    cohortIds <- dataSource$cohortTable$cohortId
  }
  
  if (is.null(databaseIds)) {
    databaseIds <- dataSource$dbTable$databaseId
  }
  
  ## cd incidence rate result-----
  cohortDiagnosticsIncidenceRateResult <-
    resultsExtractCohortDiagnosticsIncidenceRate(
      dataSource = dataSource,
      cohortIds = cohortIds,
      databaseIds = databaseIds,
      stratifyByGender = TRUE,
      stratifyByAgeGroup = TRUE,
      stratifyByCalendarYear = TRUE,
      minPersonYears = 0,
      minSubjectCount = 0,
      minIncidenceRate = 0
    )
  ## cd characterization-----
  cohortDiagnosticsCovariateCharacterization <-
    resultsExtractCohortDiagnosticsCharacterization(dataSource = dataSource,
                                                    cohortIds = cohortIds,
                                                    databaseIds = databaseIds)
  
  ## concepts in data----
  cohortDiagnosticsConceptsInCohort <-
    resultsExtractCohortDiagnosticsConceptsInCohort(dataSource = dataSource,
                                                    cohortIds = cohortIds,
                                                    databaseId = databaseIds)
  
  ## cd index event breakdown
  cohortDiagnosticsIndexEventBreakdown <-
    resultsExtractCohortDiagnosticsIndexEventBreakdown(dataSource = dataSource,
                                                       cohortIds = cohortIds,
                                                       databaseIds = databaseIds)
  
  cohortDiagnosticsResults <- c()
  cohortDiagnosticsResults$dataSource <- dataSource
  cohortDiagnosticsResults$incidenceRateResult <-
    cohortDiagnosticsIncidenceRateResult
  cohortDiagnosticsResults$covariateCharacterization <-
    cohortDiagnosticsCovariateCharacterization
  cohortDiagnosticsResults$conceptsInCohort <-
    cohortDiagnosticsConceptsInCohort
  cohortDiagnosticsResults$indexEventBreakdown <-
    cohortDiagnosticsIndexEventBreakdown
  
  if (!is.null(outputFolder)) {
    dir.create(path = outputFolder,
               showWarnings = FALSE,
               recursive = TRUE)
    saveRDS(
      object = cohortDiagnosticsResults,
      file = file.path(outputFolder, "CohortDiagnosticsResults.RDS")
    )
  }
  
  return(cohortDiagnosticsResults)
}
