#' @export
resultsExtractConnection <- function(connectionDetails) {
  ResultModelManager::PooledConnectionHandler$new(connectionDetails)
}



#' @export
resultsExtractCohortDiagnosticsDataSource <- function(connection,
                                                      resultsSchema,
                                                      resultDatabaseSettings = NULL) {
  if (is.null(resultDatabaseSettings)) {
    resultDatabaseSettings <-
      ShinyAppBuilder::createDefaultResultDatabaseSettings(schema = resultsSchema)
  }
  ## cd data source----
  cohortDiagnosticsDataSource <-
    OhdsiShinyModules::createCdDatabaseDataSource(
      connectionHandler = connection,
      resultDatabaseSettings = ShinyAppBuilder::createDefaultResultDatabaseSettings(schema = resultsSchema)
    )

  cohortDiagnosticsDataSource$cohortDefinitionSet <-
    DatabaseConnector::renderTranslateQuerySql(
      connection = connection$con,
      sql = "SELECT * FROM @results_schema.@prefixcohort;",
      results_schema = resultDatabaseSettings$schema,
      prefix = resultDatabaseSettings$cdTablePrefix,
      snakeCaseToCamelCase = TRUE
    ) |>
    dplyr::tibble()

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

    if (!is.null(minIncidenceRate)) {
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
           databaseIds,
           meanThreshold = 0.005) {
    ## cd characterization result-----
    cohortDiagnosticsCharacterizationResult <-
      OhdsiShinyModules:::getCharacterizationOutput(
        dataSource = dataSource,
        cohortIds = cohortIds,
        databaseIds = databaseIds,
        meanThreshold = meanThreshold
      ) # this is the default value

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
resultsExtractCohortDiagnosticsCohortOverlap <- function(dataSource,
                                                         cohortIds = NULL,
                                                         comparatorCohortIds = NULL,
                                                         databaseIds = NULL,
                                                         startDays = NULL,
                                                         endDays = NULL) {
  # Returns data from cohort_relationships table of Cohort Diagnostics results data model
  data <- OhdsiShinyModules:::getResultsCohortRelationships(
    dataSource = dataSource,
    cohortIds = cohortIds,
    comparatorCohortIds = comparatorCohortIds,
    databaseIds = databaseIds,
    startDays = startDays,
    endDays = endDays
  )
  return(data)
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
    resultsExtractCohortDiagnosticsDataSource(
      connection = connection,
      resultsSchema = resultsSchema
    )

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
    resultsExtractCohortDiagnosticsCharacterization(
      dataSource = dataSource,
      cohortIds = cohortIds,
      databaseIds = databaseIds,
      meanThreshold = 0.01
    )

  ## cd cohort overalp----
  cohortDiagnosticsCohortOverlap <-
    resultsExtractCohortDiagnosticsCohortOverlap(
      dataSource = dataSource,
      cohortIds = cohortIds,
      databaseIds = databaseIds
    )

  ## concepts in data----
  cohortDiagnosticsConceptsInCohort <-
    resultsExtractCohortDiagnosticsConceptsInCohort(
      dataSource = dataSource,
      cohortIds = cohortIds,
      databaseId = databaseIds
    )

  ## cd index event breakdown
  cohortDiagnosticsIndexEventBreakdown <-
    resultsExtractCohortDiagnosticsIndexEventBreakdown(
      dataSource = dataSource,
      cohortIds = cohortIds,
      databaseIds = databaseIds
    )

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
  cohortDiagnosticsResults$cohortOverlap <-
    cohortDiagnosticsCohortOverlap

  if (!is.null(outputFolder)) {
    dir.create(
      path = outputFolder,
      showWarnings = FALSE,
      recursive = TRUE
    )
    saveRDS(
      object = cohortDiagnosticsResults,
      file = file.path(outputFolder, "CohortDiagnosticsResults.RDS")
    )
  }

  return(cohortDiagnosticsResults)
}
