#' @export
getTemporalDays <- function(groupingDays = 30,
                            maxDays = 1000,
                            minDays = NULL) {
  sequence <- c(seq(
    from = (-groupingDays * round(maxDays / groupingDays)),
    to = (groupingDays * round(maxDays / groupingDays)),
    by = groupingDays
  ))

  if (!is.null(minDays)) {
    sequence <- sequence[sequence >= minDays]
  }
  return(sequence |> sort())
}

#' @export
convertCohortIdToCovariateId <- function(cohortIds,
                                         cohortCovariateAnalysisId = 150) {
  c((cohortIds * 1000) + cohortCovariateAnalysisId)
}


# getCovariateCoOccurrence: Analyzes co-occurrence of covariates within a cohort
#
# This function processes temporal covariate data to analyze the co-occurrence of covariates within specified cohorts.
# It allows filtering based on entry and exit cohort IDs and handles temporal aspects of the data.
#
# Args:
#   covariateData: A data frame representing temporal covariate data.
#   exitCohortIds: Optional vector of exit cohort IDs for filtering.
#   entryCohortIds: Optional vector of entry cohort IDs for filtering.
#   eventCohortsOfInterest: a data frame.
#
# Returns:
#   A data frame with the processed covariate co-occurrence data.
#' @export
getCovariateCoOccurrence <- function(covariateData,
                                     exitCohortIds = NULL,
                                     entryCohortIds = NULL,
                                     eventCohortsOfInterest,
                                     returnPersonLevelData = FALSE,
                                     reduced = TRUE,
                                     flagCohortIds = NULL) {
  # Asserting input validity
  checkmate::assertIntegerish(exitCohortIds, null.ok = TRUE)
  checkmate::assertIntegerish(entryCohortIds, null.ok = TRUE)

  eventCohortIds <- eventCohortsOfInterest$cohortId

  # Check if covariate data is temporal
  if (!FeatureExtraction::isTemporalCovariateData(covariateData)) {
    stop("covariateData is not a temporal covariate data object")
  }

  # Handling exit cohorts
  if (!is.null(exitCohortIds)) {
    exitCovariateIds <- convertCohortIdToCovariateId(exitCohortIds)

    # Determine the maximum time ID for each row ID in exit cohorts
    rowIdMaxTimeId <- covariateData$covariates |>
      dplyr::filter(covariateId %in% exitCovariateIds) |>
      dplyr::group_by(rowId) |>
      dplyr::summarise(maxTimeId = min(timeId, na.rm = TRUE)) |>
      dplyr::ungroup()

    # Filter covariates based on time ID constraints
    covariateData$covariates <- covariateData$covariates |>
      dplyr::left_join(rowIdMaxTimeId, by = "rowId") |>
      dplyr::filter(timeId < maxTimeId | is.na(maxTimeId)) |>
      dplyr::select(-maxTimeId)

    # Remove unwanted cohort IDs
    cohortIdsToRemove <-
      setdiff(exitCohortIds, eventCohortIds)

    covariateCohortIdsToRemove <-
      convertCohortIdToCovariateId(
        cohortIds = cohortIdsToRemove,
        cohortCovariateAnalysisId = analysisId
      )

    # Filter out the unwanted cohorts from covariates and covariate reference
    covariateData$covariates <- covariateData$covariates |>
      dplyr::filter(!covariateId %in% covariateCohortIdsToRemove)

    covariateData$covariateRef <- covariateData$covariateRef |>
      dplyr::filter(!covariateId %in% covariateCohortIdsToRemove)
  }

  # Handling entry cohorts
  if (!is.null(entryCohortIds)) {
    entryCovariateIds <-
      convertCohortIdToCovariateId(
        cohortIds = entryCohortIds,
        cohortCovariateAnalysisId = analysisId
      )

    # Determine the minimum time ID for each row ID in entry cohorts
    rowIdMinTimeId <- covariateData$covariates |>
      dplyr::filter(covariateId %in% entryCovariateIds) |>
      dplyr::group_by(rowId) |>
      dplyr::summarise(minTimeId = min(timeId, na.rm = TRUE)) |>
      dplyr::ungroup()

    # Filter covariates based on time ID constraints for entry cohorts
    covariateData$covariates <- covariateData$covariates |>
      dplyr::left_join(rowIdMinTimeId, by = "rowId") |>
      dplyr::filter(timeId >= minTimeId) |>
      dplyr::select(-minTimeId)

    # Remove unwanted cohort IDs for entry cohorts
    cohortIdsToRemove <-
      setdiff(exitCohortIds, eventCohortIds)

    covariateCohortIdsToRemove <-
      convertCohortIdToCovariateId(
        cohortIds = entryCohortIds,
        cohortCovariateAnalysisId = analysisId
      )

    covariateData$covariates <- covariateData$covariates |>
      dplyr::filter(!covariateId %in% covariateCohortIdsToRemove)

    covariateData$covariateRef <- covariateData$covariateRef |>
      dplyr::filter(!covariateId %in% covariateCohortIdsToRemove)
  }

  # Extract population size from metadata
  populationSize <-
    attr(x = covariateData, which = "metaData")$populationSize

  # Process time reference for readability
  timeRef <- covariateData$timeRef |>
    dplyr::collect() |>
    dplyr::mutate(timeLabel = paste0(startDay, "d to ", endDay, "d")) |>
    dplyr::select(timeId, timeLabel) |>
    dplyr::ungroup()

  # Collect analysis reference
  analysisRef <- covariateData$analysisRef |>
    dplyr::collect() |>
    dplyr::ungroup()

  # Process cohort reference for readability
  covariateRef <- covariateData$covariateRef |>
    dplyr::collect() |>
    dplyr::mutate(covariateName = stringr::str_extract(covariateName, "(?<=: )\\w+")) |>
    dplyr::mutate(cohortId = (covariateId - analysisId) / 1000) |>
    dplyr::select(cohortId, covariateId, covariateName) |>
    dplyr::rename(cohortName = covariateName) |>
    dplyr::distinct() |>
    dplyr::arrange(cohortId) |>
    dplyr::inner_join(
      eventCohortsOfInterest |>
        dplyr::select(
          covariateId,
          newCovariateId,
          newCovariateName
        ) |>
        dplyr::distinct(),
      by = "covariateId"
    )

  # Filter cohorts if covariate ID group is specified
  if (!is.null(flagCohortIds)) {
    flagCovariateIds <-
      dplyr::tibble(
        covariateId = convertCohortIdToCovariateId(
          cohortIds = flagCohortIds,
          cohortCovariateAnalysisId = analysisRef$analysisId
        ),
        flag = "1"
      )

    covariateRef <- covariateRef |>
      dplyr::left_join(flagCovariateIds,
        by = "covariateId"
      ) |>
      tidyr::replace_na(replace = list(flag = "0"))
  } else {
    covariateRef <- covariateRef |>
      dplyr::mutate(flag = "1")
  }

  # Generate combinations of covariates for each person and time
  covariateCoOccurrenceData <- covariateData$covariates |>
    dplyr::select(rowId, covariateId, timeId) |>
    dplyr::rename(id = rowId) |>
    dplyr::collect() |>
    dplyr::inner_join(covariateRef,
      by = "covariateId"
    ) |>
    dplyr::select(
      id,
      timeId,
      newCovariateId,
      flag
    ) |>
    dplyr::group_by(id, timeId, newCovariateId) |>
    dplyr::summarise(flag = max(flag), .groups = "keep") |>
    dplyr::ungroup() |>
    dplyr::arrange(id, timeId, newCovariateId) |>
    dplyr::group_by(id, timeId) |>
    dplyr::summarise(
      covariateCombinationId = paste(sort(newCovariateId), collapse = ", "),
      flag = max(flag)
    ) |>
    dplyr::ungroup() |>
    dplyr::arrange(id, timeId)

  # Aggregate and identify rare covariate combinations
  covariateCoOccurrenceRef <- covariateCoOccurrenceData |>
    dplyr::group_by(timeId, covariateCombinationId) |>
    dplyr::summarise(count = dplyr::n_distinct(id)) |>
    dplyr::arrange(dplyr::desc(count)) |>
    dplyr::mutate(rn = dplyr::row_number()) |>
    dplyr::mutate(isRare = dplyr::if_else(
      condition = (rn <= (maxFeatures - 1)),
      true = 0,
      false = 1
    )) |>
    dplyr::select(timeId, covariateCombinationId, isRare, count) |>
    dplyr::distinct()

  # Function to replace ID with covariate name
  replaceWithCovariateName <- function(id) {
    covariateRefToMatch <- covariateRef |>
      dplyr::select(
        newCovariateId,
        newCovariateName
      ) |>
      dplyr::distinct() |>
      dplyr::arrange(newCovariateId)
    matchIdx <-
      min(which(covariateRefToMatch$newCovariateId == as.numeric(id)))
    if (length(matchIdx) > 0) {
      return(covariateRefToMatch$newCovariateName[matchIdx])
    } else {
      return(id)
    }
  }

  # Replace strata IDs with covariate names
  replaceStrataIds <- function(covariateCoOccurrenceRef) {
    covariateCoOccurrenceRef$covariateCombination <-
      sapply(covariateCoOccurrenceRef$covariateCombinationId, function(covariateCombinationId) {
        ids <-
          unlist(strsplit(as.character(covariateCombinationId), split = ","))
        names <- sapply(ids, replaceWithCovariateName)
        paste(names, collapse = ", ")
      })

    covariateCoOccurrenceRef <- covariateCoOccurrenceRef |>
      dplyr::mutate(
        covariateCombinationReduced =
          dplyr::if_else(
            condition = (isRare == 1),
            true = "other",
            false = covariateCombination
          )
      )
    return(covariateCoOccurrenceRef)
  }

  result <- c()
  result$populationSize <- populationSize

  if (returnPersonLevelData) {
    result$covariateCoOccurrenceData <- covariateCoOccurrenceData |>
      dplyr::arrange(
        id,
        timeId,
        covariateCombinationId
      )
  }
  result$covariateRef <- covariateRef
  result$timeRef <- timeRef
  result$analysisRef <- analysisRef
  result$covariateCoOccurrenceRef <-
    replaceStrataIds(covariateCoOccurrenceRef)

  if (reduced) {
    result$sunburstRData <- covariateCoOccurrenceData |>
      dplyr::inner_join(
        result$covariateCoOccurrenceRef |>
          dplyr::select(
            timeId,
            covariateCombinationId,
            covariateCombinationReduced
          ) |>
          dplyr::rename(covariateCombination = covariateCombinationReduced),
        by = c(
          "timeId",
          "covariateCombinationId"
        )
      ) |>
      dplyr::arrange(
        id,
        timeId
      ) |>
      tidyr::pivot_wider(
        id_cols = c(id),
        names_prefix = "node",
        names_from = timeId,
        values_from = covariateCombination
      ) |>
      dplyr::group_by(dplyr::across(dplyr::starts_with("node"))) |> # Group by columns starting with "level"
      dplyr::summarize(size = dplyr::n(), .groups = "keep") |>
      dplyr::ungroup() |>
      dplyr::arrange(dplyr::desc(size)) |>
      d3r::d3_nest(value_cols = "size")
  } else {
    result$sunburstRData <- covariateCoOccurrenceData |>
      dplyr::inner_join(
        result$covariateCoOccurrenceRef |>
          dplyr::select(
            timeId,
            covariateCombinationId,
            covariateCombination
          ),
        by = c(
          "timeId",
          "covariateCombinationId"
        )
      ) |>
      dplyr::arrange(
        id,
        timeId
      ) |>
      tidyr::pivot_wider(
        id_cols = c(id),
        names_prefix = "node",
        names_from = timeId,
        values_from = covariateCombination
      ) |>
      dplyr::group_by(dplyr::across(dplyr::starts_with("node"))) |> # Group by columns starting with "level"
      dplyr::summarize(size = dplyr::n(), .groups = "keep") |>
      dplyr::ungroup() |>
      dplyr::arrange(dplyr::desc(size)) |>
      d3r::d3_nest(value_cols = "size")
  }

  if (reduced) {
    ggAlluvialLodeForm <-
      result$covariateCoOccurrenceData |>
      dplyr::inner_join(
        result$covariateCoOccurrenceRef |>
          dplyr::select(
            timeId,
            covariateCombinationId,
            covariateCombinationReduced
          ) |>
          dplyr::rename(covariateCombination = covariateCombinationReduced),
        by = c(
          "timeId",
          "covariateCombinationId"
        )
      ) |>
      dplyr::arrange(
        id,
        timeId
      ) |>
      tidyr::pivot_wider(
        id_cols = c(id, flag),
        names_prefix = "node",
        names_from = timeId,
        values_from = covariateCombination
      ) |>
      dplyr::group_by(flag, dplyr::across(dplyr::starts_with("node"))) |> # Group by columns starting with "level"
      dplyr::summarize(size = dplyr::n(), .groups = "keep") |>
      dplyr::ungroup() |>
      dplyr::arrange(flag, dplyr::desc(size))
  } else {
    ggAlluvialLodeForm <-
      result$covariateCoOccurrenceData |>
      dplyr::inner_join(
        result$covariateCoOccurrenceRef |>
          dplyr::select(
            timeId,
            covariateCombinationId,
            covariateCombination
          ),
        by = c(
          "timeId",
          "covariateCombinationId"
        )
      ) |>
      dplyr::arrange(
        id,
        timeId
      ) |>
      tidyr::pivot_wider(
        id_cols = c(id, flag),
        names_prefix = "node",
        names_from = timeId,
        values_from = covariateCombination
      ) |>
      dplyr::group_by(flag, dplyr::across(dplyr::starts_with("node"))) |> # Group by columns starting with "level"
      dplyr::summarize(size = dplyr::n(), .groups = "keep") |>
      dplyr::ungroup() |>
      dplyr::arrange(flag, dplyr::desc(size))
  }

  colPositions <- which(grepl("node", names(ggAlluvialLodeForm)))

  result$ggAlluvialLodeForm <- ggAlluvialLodeForm |>
    ggalluvial::to_lodes_form(key = "state", axes = colPositions)

  return(result)
}
