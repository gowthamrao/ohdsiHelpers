#' Calculate Temporal Relationships Between OHDSI Cohorts
#'
#' This function assesses the temporal relationships between a target OHDSI cohort
#' and multiple covariate cohorts. It does this by examining the start and end 
#' dates of the covariate cohorts in relation to the start date of the target cohort.
#' The function generates a series of time windows based on `groupingDays` and 
#' `maxDays`, and checks for overlaps of the cohorts within these windows.
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
