#' @export
getTemporalDays <- function(groupingDays = 30,
                            maxDays = 1000) {
  c(seq(
    from = (-groupingDays * round(maxDays / groupingDays)),
    to = (groupingDays * round(maxDays / groupingDays)),
    by = groupingDays
  ))
}

#' Get temporal overlap between covariate and target cohorts
#'
#' This function assesses the temporal relationships between a target cohort
#' and multiple covariate cohorts by looking for overlap of covariate cohort
#' in a time window in relation to target cohort start date.
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
                                     startDays,
                                     endDays,
                                     targetCohortId) {
  # Assert checks using checkmate
  checkmate::assertList(connectionDetails, null.ok = TRUE)
  checkmate::assertCharacter(cohortDatabaseSchema)
  checkmate::assertCharacter(cdmDatabaseSchema)
  checkmate::assertIntegerish(cohortCovariateAnalysisId, len = 1)
  checkmate::assertCharacter(cohortTable)
  checkmate::assertIntegerish(covariateCohortIds)
  checkmate::assertIntegerish(exitCohortIds, null.ok = TRUE)
  checkmate::assertIntegerish(entryCohortIds, null.ok = TRUE)
  checkmate::assertIntegerish(groupingDays, lower = 1)
  checkmate::assertIntegerish(maxDays, lower = 1)
  checkmate::assertIntegerish(targetCohortId, len = 1)
  
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
    temporalStartDays = startDays,
    temporalEndDays = endDays
  )
  
  if (!is.null(exitCohortIds)) {
    exitCovariateIds <-
      c((exitCohortIds * 1000) + cohortCovariateAnalysisId)
    
    rowIdMaxTimeId <- covariateData$covariates |>
      dplyr::filter(covariateId %in% exitCovariateIds) |>
      dplyr::group_by(rowId) |>
      dplyr::summarise(maxTimeId = min(timeId, na.rm = TRUE)) |> ## max time id is earliest timeId for any exitCohortId
      dplyr::ungroup()
    
    covariateData$covariates <- covariateData$covariates |>
      dplyr::left_join(rowIdMaxTimeId, by = "rowId") |>
      dplyr::filter(timeId < maxTimeId | is.na(maxTimeId)) |>
      dplyr::select(-maxTimeId)
  }
  
  if (!is.null(entryCohortIds)) {
    entryCovariateIds <-
      c((entryCohortIds * 1000) + cohortCovariateAnalysisId)
    
    rowIdMaxTimeId <- covariateData$covariates |>
      dplyr::filter(covariateId %in% entryCovariateIds) |>
      dplyr::group_by(rowId) |>
      dplyr::summarise(maxTimeId = max(timeId, na.rm = TRUE)) |> ## max time id is latest timeId for any exitCohortId
      dplyr::ungroup()
    
    covariateData$covariates <- covariateData$covariates |>
      dplyr::left_join(rowIdMaxTimeId, by = "rowId") |>
      dplyr::filter(timeId >= maxTimeId |
                      is.na(maxTimeId)) |>  #on or after
      dplyr::select(-maxTimeId)
  }
  
  return(covariateData)
}
