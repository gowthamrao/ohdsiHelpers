#' Get temporal overlap between covariate and target cohorts
#'
#' This function assesses the temporal relationships between a target cohort
#' and multiple covariate cohorts by looking for overlap of covariate cohort 
#' in a time window in relation to target cohort start date.
#' 
#' The function generates a series of equal time windows based on `groupingDays` and 
#' `maxDays`, where `groupingDays` defines the length of each time window and 
#' `maxDays` determines the overall period under consideration. 
#' 
#' Overlap is calculated using FeatureExtraction.
#'
#' @param connection An optional database connection object.
#' @param connectionDetails An object containing the database connection details.
#' @param cohortDatabaseSchema The name of the database schema containing the cohort.
#' @param cdmDatabaseSchema The name of the CDM database schema.
#' @param cohortCovariateAnalysisId The ID for cohort covariate analysis (default 150).
#' @param cohortTable The name of the table containing the cohort data.
#' @param covariateCohortDefinitionSet Definitions for covariate cohorts.
#' @param covariateCohortIds A vector of IDs for the covariate cohorts.
#' @param exitCohortIds Optional vector of exit cohort IDs.
#' @param entryCohortIds Optional vector of entry cohort IDs.
#' @param groupingDays The number of days for grouping in the time window (default 30).
#' @param maxDays The maximum number of days for the time window range (default 1000).
#' @param targetCohortId The ID of the target cohort.
#' @return A data frame containing the covariate data.
#' @export
#' @examples
#' # Example usage
#' getCohortTemporalOverlap(connection = dbConnection,
#'                          connectionDetails = connectionDetails,
#'                          cohortDatabaseSchema = "myCohortSchema",
#'                          cdmDatabaseSchema = "myCdmSchema",
#'                          targetCohortId = 123)
getCohortTemporalOverlap <- function(connection = NULL,
                                     connectionDetails = NULL,
                                     cohortDatabaseSchema,
                                     cdmDatabaseSchema,
                                     cohortCovariateAnalysisId = 150,
                                     cohortTable,
                                     covariateCohortDefinitionSet,
                                     covariateCohortIds,
                                     exitCohortIds = NULL,
                                     entryCohortIds = NULL,
                                     groupingDays = 30,
                                     maxDays = 1000,
                                     targetCohortId) {
  
  # Assert checks using checkmate
  checkmate::assertList(connectionDetails, null.ok = TRUE)
  checkmate::assertCharacter(cohortDatabaseSchema)
  checkmate::assertCharacter(cdmDatabaseSchema)
  checkmate::assertIntegerish(cohortCovariateAnalysisId, len = 1)
  checkmate::assertCharacter(cohortTable)
  checkmate::assertTRUE(CohortGenerator::isCohortDefinitionSet(covariateCohortDefinitionSet))
  checkmate::assertIntegerish(covariateCohortIds)
  checkmate::assertIntegerish(exitCohortIds, null.ok = TRUE)
  checkmate::assertIntegerish(entryCohortIds, null.ok = TRUE)
  checkmate::assertIntegerish(groupingDays, lower = 1)
  checkmate::assertIntegerish(maxDays, lower = 1)
  checkmate::assertIntegerish(targetCohortId, len = 1)
  
  # Calculate the start days for the temporal windows
  temporalStartDays <- c(seq(
    from = (-groupingDays * round(maxDays / groupingDays)),
    to = (groupingDays * round(maxDays / groupingDays)),
    by = groupingDays
  )) + 1
  
  # Calculate the end days for the temporal windows
  temporalEndDays <- temporalStartDays + (groupingDays - 1)
  
  # Extract covariate data using OHDSI helpers
  covariateData <- OhdsiHelpers::executeCohortFeatureExtraction(
    connectionDetails = connectionDetails,
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortDatabaseSchema = cohortDatabaseSchema,
    cohortIds = targetCohortId,
    cohortTable = cohortTable,
    covariateCohortDefinitionSet = covariateCohortDefinitionSet,
    covariateCohortIds = c(covariateCohortIds, exitCohortIds, entryCohortIds),
    cohortCovariateAnalysisId = cohortCovariateAnalysisId,
    aggregated = FALSE,
    temporalStartDays = temporalStartDays,
    temporalEndDays = temporalEndDays
  )
  
  browser()
  return(covariateData)
}
