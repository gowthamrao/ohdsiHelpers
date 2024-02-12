#' @export
getCovaraiteDataSummaryByTimeId <- function(covariateData,
                                            cohortCovariateAnalysisId,
                                            covariateCohortId = NULL,
                                            startDay = NULL) {
  # Validate input parameters
  if (!FeatureExtraction::isTemporalCovariateData(covariateData)) {
    stop("Must be a tmeporal covariateData object")
  }
  if (!is.null(cohortCovariateAnalysisId) &&
      !is.numeric(cohortCovariateAnalysisId)) {
    stop("cohortCovariateAnalysisId must be numeric")
  }
  if (!is.null(covariateCohortId) &&
      !is.numeric(covariateCohortId)) {
    stop("covariateCohortId must be numeric")
  }
  if (!is.null(startDay) && !is.numeric(startDay)) {
    stop("startDay must be numeric")
  }
  
  # Extract population size and convert to numeric
  populationSize <-
    ((attr(x = covariateData, which = "metaData")$populationSize) |> as.numeric())
  
  # Select covariates and exclude covariateValue
  covariates <- covariateData$covariates |>
    dplyr::select(-covariateValue)
  
  # Filter covariates based on startDay, if provided
  if (!is.null(startDay)) {
    covariates <-
      covariates |> dplyr::inner_join(covariateData$timeRef |>
                                        dplyr::filter(startDay >= 0),
                                      by = "timeId")
  } else {
    covariates <- covariates |> dplyr::inner_join(covariateData$timeRef,
                                                  by = "timeId")
  }
  
  if (!is.null(cohortCovariateAnalysisId)) {
    covariates <- covariates |>
      dplyr::filter(covariateId %in% c((featureCohortIds * 1000) + cohortCovariateAnalysisId))
  }
  
  covariates <- covariates |> 
    dplyr::filter(covariateId > 0)
  
  # Calculate summary statistics for covariates
  outputAll <- suppressWarnings(
    covariates |>
      dplyr::select(-timeId) |>
      dplyr::group_by(rowId, covariateId) |>
      dplyr::summarise(
        startDay = min(startDay),
        endDay = min(endDay),
        .groups = "drop"
      ) |>
      dplyr::distinct() |>
      dplyr::arrange(startDay, endDay, covariateId) |>
      dplyr::group_by(startDay, endDay, covariateId) |>
      dplyr::summarise(rowCount = dplyr::n_distinct(rowId)) |>
      dplyr::ungroup() |>
      dplyr::group_by(covariateId) |>
      dplyr::mutate(cumulativeSum = cumsum(rowCount)) |>
      dplyr::ungroup() |>
      dplyr::arrange(startDay, endDay, covariateId) |>
      dplyr::mutate(
        proportion = round((rowCount / populationSize), 2),
        cumulativeProportion = round((cumulativeSum / populationSize), 2)
      ) |>
      dplyr::arrange(covariateId,
                     startDay,
                     endDay) |>
      dplyr::collect()
  )
  
  outputAggegated <- suppressWarnings(
    covariates |>
      dplyr::select(-timeId) |>
      dplyr::group_by(rowId) |>
      dplyr::summarise(
        startDay = min(startDay),
        endDay = min(endDay),
        .groups = "drop"
      ) |>
      dplyr::distinct() |>
      dplyr::arrange(startDay, endDay) |>
      dplyr::group_by(startDay, endDay) |>
      dplyr::summarise(rowCount = dplyr::n_distinct(rowId)) |>
      dplyr::ungroup() |>
      dplyr::mutate(cumulativeSum = cumsum(rowCount)) |>
      dplyr::ungroup() |>
      dplyr::arrange(startDay, endDay) |>
      dplyr::mutate(
        proportion = round((rowCount / populationSize), 2),
        cumulativeProportion = round((cumulativeSum / populationSize), 2)
      ) |>
      dplyr::arrange(startDay,
                     endDay) |>
      dplyr::collect() |>
      dplyr::mutate(covariateId = 0)
  )
  
  return(dplyr::bind_rows(output,
                          outputAggegated))
}
