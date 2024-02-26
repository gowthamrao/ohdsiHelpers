#' @export
getCovaraiteDataSummaryByTimeId <- function(covariateData,
                                            cohortCovariateAnalysisId = 150,
                                            covariateCohortId = NULL,
                                            startDay = NULL,
                                            minProportion = NULL,
                                            minCount = NULL,
                                            getCohortId = TRUE,
                                            cohortDefinitionSet) {
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
  if (!is.null(covariateCohortId) &&
    is.null(cohortCovariateAnalysisId)) {
    stop("cannot filter by covariateCohortId. cohortCovariateAnalysisId should be provided")
  }
  if (!is.null(startDay) && !is.numeric(startDay)) {
    stop("startDay must be numeric")
  }

  # Extract population size and convert to numeric
  populationSize <-
    ((attr(x = covariateData, which = "metaData")$populationSize) |> as.numeric())

  # Select covariates and exclude covariateValue
  covariates <- covariateData$covariates |>
    dplyr::select(-.data$covariateValue)

  # Filter covariates based on startDay, if provided
  if (!is.null(startDay)) {
    covariates <-
      covariates |> dplyr::inner_join(
        covariateData$timeRef |>
          dplyr::filter(.data$startDay >= 0),
        by = "timeId"
      )
  } else {
    covariates <- covariates |> dplyr::inner_join(covariateData$timeRef,
      by = "timeId"
    )
  }

  if (!is.null(cohortCovariateAnalysisId)) {
    if (!is.null(covariateCohortId)) {
      filterCovariateIds <-
        ((cohortDefinitionSet$cohortId * 1000) + cohortCovariateAnalysisId) |> unique()
      covariates <- covariates |>
        dplyr::filter(.data$covariateId %in% filterCovariateIds)
    }
  }

  covariates <- covariates |>
    dplyr::filter(.data$covariateId > 0)

  # Calculate summary statistics for covariates
  outputAll <- suppressWarnings(
    covariates |>
      dplyr::select(-.data$timeId) |>
      dplyr::group_by(.data$rowId, .data$covariateId) |>
      dplyr::summarise(
        startDay = min(.data$startDay),
        endDay = min(.data$endDay),
        .groups = "drop"
      ) |>
      dplyr::distinct() |>
      dplyr::arrange(.data$startDay, .data$endDay, .data$covariateId) |>
      dplyr::group_by(.data$startDay, .data$endDay, .data$covariateId) |>
      dplyr::summarise(rowCount = dplyr::n_distinct(.data$rowId)) |>
      dplyr::ungroup() |>
      dplyr::group_by(.data$covariateId) |>
      dplyr::mutate(cumulativeSum = cumsum(.data$rowCount)) |>
      dplyr::ungroup() |>
      dplyr::arrange(.data$startDay, .data$endDay, .data$covariateId) |>
      dplyr::mutate(
        proportion = round((.data$rowCount / .data$populationSize), 2),
        cumulativeProportion = round((.data$cumulativeSum / .data$populationSize), 2)
      ) |>
      dplyr::arrange(
        .data$covariateId,
        .data$startDay,
        .data$endDay
      ) |>
      dplyr::collect()
  )

  outputAggegated <- suppressWarnings(
    covariates |>
      dplyr::select(-.data$timeId) |>
      dplyr::group_by(.data$rowId) |>
      dplyr::summarise(
        startDay = min(.data$startDay),
        endDay = min(.data$endDay),
        .groups = "drop"
      ) |>
      dplyr::distinct() |>
      dplyr::arrange(.data$startDay, .data$endDay) |>
      dplyr::group_by(.data$startDay, .data$endDay) |>
      dplyr::summarise(rowCount = dplyr::n_distinct(.data$rowId)) |>
      dplyr::ungroup() |>
      dplyr::mutate(cumulativeSum = cumsum(.data$rowCount)) |>
      dplyr::ungroup() |>
      dplyr::arrange(.data$startDay, .data$endDay) |>
      dplyr::mutate(
        proportion = round((.data$rowCount / .data$populationSize), 2),
        cumulativeProportion = round((.data$cumulativeSum / .data$populationSize), 2)
      ) |>
      dplyr::arrange(
        .data$startDay,
        .data$endDay
      ) |>
      dplyr::collect() |>
      dplyr::mutate(covariateId = (1000 + .data$cohortCovariateAnalysisId))
  )

  output <- dplyr::bind_rows(
    outputAll,
    outputAggegated
  ) |>
    dplyr::arrange(
      .data$covariateId,
      .data$startDay,
      .data$endDay
    ) |>
    dplyr::relocate(
      .data$covariateId,
      .data$startDay,
      .data$endDay
    )

  if (!is.null(minProportion)) {
    output <- output |>
      dplyr::filter(.data$proportion > minProportion)
  }
  if (!is.null(minCount)) {
    output <- output |>
      dplyr::filter(.data$rowCount > minCount)
  }

  if (getCohortId) {
    output <- output |>
      dplyr::mutate(cohortId = (.data$covariateId - .data$cohortCovariateAnalysisId) / 1000) |>
      dplyr::relocate(.data$cohortId)
  }

  if (!is.null(cohortDefinitionSet)) {
    output <- output |>
      dplyr::left_join(cohortDefinitionSet,
        by = "cohortId"
      )
  }
  return(output)
}
