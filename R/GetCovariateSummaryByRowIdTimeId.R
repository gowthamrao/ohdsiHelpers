#' @export
getTemporalDays <- function(groupingDays = 30,
                            maxDays = 1000) {
  c(seq(
    from = (-groupingDays * round(maxDays / groupingDays)),
    to = (groupingDays * round(maxDays / groupingDays)),
    by = groupingDays
  ))
}

#' @export
convertCohortIdToCovariateId <- function(cohortIds,
                                         cohortCovariateAnalysisId = 150) {
  c((cohortIds * 1000) + cohortCovariateAnalysisId)
}


#' Get temporal overlap between covariate and target cohorts
#'
#' This function assesses the temporal relationships between a target cohort
#' and multiple covariate cohorts by looking for overlap of covariate cohort
#' in a time window in relation to target cohort start date.
#'
#' Overlap is calculated using FeatureExtraction.
#'
#' @param covariateData A feature extraction output.
#' @param eventCohortIds vector of event cohort IDs.
#' @param exitCohortIds Optional vector of exit cohort IDs.
#' @param entryCohortIds Optional vector of entry cohort IDs.
#' @return A data frame containing the covariate data.
#' @export
#' @examples
#' # Example usage
#' getCovariateSummaryByRowIdTimeId(connection = dbConnection,
#'                          connectionDetails = connectionDetails,
#'                          cohortDatabaseSchema = "myCohortSchema",
#'                          cdmDatabaseSchema = "myCdmSchema")
getCovariateSummaryByRowIdTimeId <- function(covariateData,
                                             exitCohortIds = NULL,
                                             entryCohortIds = NULL,
                                             eventCohortIds) {
  checkmate::assertIntegerish(exitCohortIds, null.ok = TRUE)
  checkmate::assertIntegerish(entryCohortIds, null.ok = TRUE)
  
  if (!FeatureExtraction::isTemporalCovariateData(covariateData)) {
    stop("covariateData is not temporal covariate data object")
  }
  
  if (!is.null(exitCohortIds)) {
    exitCovariateIds <- convertCohortIdToCovariateId(exitCohortIds)
    
    rowIdMaxTimeId <- covariateData$covariates |>
      dplyr::filter(covariateId %in% exitCovariateIds) |>
      dplyr::group_by(rowId) |>
      dplyr::summarise(maxTimeId = min(timeId, na.rm = TRUE)) |> ## max time id is earliest timeId for any exitCohortId
      dplyr::ungroup()
    
    covariateData$covariates <- covariateData$covariates |>
      dplyr::left_join(rowIdMaxTimeId, by = "rowId") |>
      dplyr::filter(timeId < maxTimeId | is.na(maxTimeId)) |>
      dplyr::select(-maxTimeId)
    
    cohortIdsToRemove <-
      setdiff(exitCohortIds, eventCohortIds)
    
    covariateCohortIdsToRemove <-
      convertCohortIdToCovariateId(cohortIds = cohortIdsToRemove,
                                   cohortCovariateAnalysisId = analysisId)
    
    covariateData$covariates <- covariateData$covariates |>
      dplyr::filter(!covariateId %in% covariateCohortIdsToRemove)
    
    covariateData$covariateRef <- covariateData$covariateRef |>
      dplyr::filter(!covariateId %in% covariateCohortIdsToRemove)
  }
  
  if (!is.null(entryCohortIds)) {
    entryCovariateIds <-
      convertCohortIdToCovariateId(cohortIds = entryCohortIds,
                                   cohortCovariateAnalysisId = analysisId)
    
    rowIdMinTimeId <- covariateData$covariates |>
      dplyr::filter(covariateId %in% entryCovariateIds) |>
      dplyr::group_by(rowId) |>
      dplyr::summarise(minTimeId = min(timeId, na.rm = TRUE)) |> ## max time id is latest timeId for any exitCohortId
      dplyr::ungroup()
    
    covariateData$covariates <- covariateData$covariates |>
      dplyr::left_join(rowIdMinTimeId, by = "rowId") |>
      dplyr::filter(timeId >= minTimeId) |>  #on or after
      dplyr::select(-minTimeId)
    
    cohortIdsToRemove <-
      setdiff(exitCohortIds, eventCohortIds)
    
    covariateCohortIdsToRemove <-
      convertCohortIdToCovariateId(cohortIds = entryCohortIds,
                                   cohortCovariateAnalysisId = analysisId)
    
    covariateData$covariates <- covariateData$covariates |>
      dplyr::filter(!covariateId %in% covariateCohortIdsToRemove)
    
    covariateData$covariateRef <- covariateData$covariateRef |>
      dplyr::filter(!covariateId %in% covariateCohortIdsToRemove)
  }
  
  populationSize <-
    attr(x = covariateData, which = "metaData")$populationSize
  
  cohortRef <- covariateData$covariateRef |>
    dplyr::collect() |>
    dplyr::mutate(
      covariateName = stringr::str_replace(
        string = covariateName,
        pattern = "cohort: ",
        replacement = ""
      )
    ) |> 
    dplyr::mutate(cohortId = (covariateId - analysisId)/1000) |> 
    dplyr::select(cohortId,
                  covariateId,
                  covariateName) |> 
    dplyr::rename(cohortName = covariateName) |> 
    dplyr::distinct() |> 
    dplyr::arrange(cohortId)
  
  timeRef <- covariateData$timeRef |>
    dplyr::collect() |>
    dplyr::filter(timeId <= maxNodes)
  
  analysisRef <- covariateData$analysisRef |>
    dplyr::collect()
  
  nodeByPerson <- covariateData$covariates |>
    dplyr::select(rowId,
                  covariateId,
                  timeId) |>
    dplyr::rename(id = rowId) |>
    dplyr::collect() |>
    dplyr::inner_join(timeId) |>
    dplyr::inner_join(covariateRef |>
                        dplyr::select(covariateId,
                                      covariateName)) |>
    dplyr::group_by(id, timeId) |>
    dplyr::summarise(state = paste(sort(covariateName), collapse = ", ")) |>
    dplyr::ungroup() |>
    dplyr::arrange(id, timeId)
  
  nodeByPerson <- nodeByPerson |>
    dplyr::left_join(
      nodeByPerson |>
        dplyr::mutate(
          hadOutcome = dplyr::if_else(
            condition = stringr::str_detect(
              string = tolower(state),
              pattern = tolower(groupCovariateText)
            ),
            true = "yes",
            false = "no"
          )
        ) |>
        dplyr::group_by(id) |>
        dplyr::summarise(hadOutcome = max(hadOutcome))
    )
  
  stateCombinations <- nodeByPerson |>
    dplyr::group_by(timeId, state) |>
    dplyr::summarise(count = dplyr::n_distinct(id)) |>
    dplyr::arrange(dplyr::desc(count)) |>
    dplyr::mutate(rn = dplyr::row_number()) |>
    dplyr::mutate(
      reducedState = dplyr::if_else(
        condition = rn <= maxFeatures - 1,
        true = state,
        false = !!rareStrataName
      )
    ) |>
    dplyr::select(timeId,
                  state,
                  reducedState) |>
    dplyr::distinct()
  
  nodeByPerson <- nodeByPerson |>
    dplyr::inner_join(stateCombinations)
  
  return(covariateData)
}
