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
#   exitCovariateIds: Optional vector of exit cohort IDs for filtering.
#   entryCovariateIds: Optional vector of entry cohort IDs for filtering.
#   eventCovariateIdsOfInterest: a data frame.
#
# Returns:
#   A data frame with the processed covariate co-occurrence data.
#' @export
getCovariateCoOccurrence <- function(covariateData,
                                     exitCovariateIds = NULL,
                                     entryCovariateIds = NULL,
                                     eventCovariateIdsOfInterest,
                                     returnPersonLevelData = FALSE,
                                     reduced = TRUE,
                                     flagCovariateIds = NULL,
                                     maxFeatures = 10) {
  if (FeatureExtraction::isAggregatedCovariateData(covariateData)) {
    stop("cannot be aggregated covariateData")
  }
  # Asserting input validity
  checkmate::assertIntegerish(exitCovariateIds, null.ok = TRUE)
  checkmate::assertIntegerish(entryCovariateIds, null.ok = TRUE)
  
  eventCovariateIds <-
    eventCovariateIdsOfInterest$covariateId |> unique()
  
  if (!'newCovariateId' %in% colnames(eventCovariateIdsOfInterest)) {
    eventCovariateIdsOfInterest <- eventCovariateIdsOfInterest |>
      dplyr::mutate(
        newCovariateId = .data$covariateId,
        newCovariateName = .data$covariateName
      )
  }
  
  # Check if covariate data is temporal
  if (!FeatureExtraction::isTemporalCovariateData(covariateData)) {
    stop("covariateData is not a temporal covariate data object")
  }
  
  covariateData$covariatesFiltered <- covariateData$covariates
  
  # Handling exit cohorts
  if (!is.null(exitCovariateIds)) {
    # Determine the maximum time ID for each row ID in exit cohorts
    rowIdMaxTimeId <- covariateData$covariatesFiltered |>
      dplyr::filter(.data$covariateId %in% exitCovariateIds) |>
      dplyr::group_by(.data$rowId) |>
      dplyr::summarise(maxTimeId = min(.data$timeId, na.rm = TRUE)) |>
      dplyr::ungroup()
    
    if (rowIdMaxTimeId |> dplyr::count() |> dplyr::collect() |> dplyr::pull() > 0) {
      # Filter covariates based on time ID constraints
      covariateData$covariatesFiltered <-
        covariateData$covariatesFiltered |>
        dplyr::left_join(rowIdMaxTimeId, by = "rowId") |>
        dplyr::filter(.data$timeId < .data$maxTimeId |
                        is.na(.data$maxTimeId)) |>
        dplyr::select(-.data$maxTimeId)
      
      # Remove unwanted cohort IDs
      covariateIdsToRemove <-
        setdiff(exitCovariateIds, eventCovariateIds)
      
      if (length(covariateIdsToRemove) > 0) {
        # Filter out the unwanted cohorts from covariates and covariate reference
        covariateData$covariatesFiltered <-
          covariateData$covariatesFiltered |>
          dplyr::filter(!.data$covariateId %in% covariateCohortIdsToRemove)
      }
    }
  }
  
  # Handling entry cohorts
  if (!is.null(entryCovariateIds)) {
    # Determine the minimum time ID for each row ID in entry cohorts
    rowIdMinTimeId <- covariateData$covariatesFiltered |>
      dplyr::filter(.data$covariateId %in% entryCovariateIds) |>
      dplyr::group_by(.data$rowId) |>
      dplyr::summarise(minTimeId = min(.data$timeId, na.rm = TRUE)) |>
      dplyr::ungroup()
    
    if (rowIdMinTimeId |> dplyr::count() |> dplyr::collect() |> dplyr::pull() > 0) {
      # Filter covariates based on time ID constraints for entry cohorts
      covariateData$covariatesFiltered <-
        covariateData$covariatesFiltered |>
        dplyr::left_join(rowIdMinTimeId, by = "rowId") |>
        dplyr::filter(.data$timeId >= .data$minTimeId) |>
        dplyr::select(-.data$minTimeId)
      
      # Remove unwanted cohort IDs for entry cohorts
      covariateIdsToRemove <-
        setdiff(exitCovariateIds, eventCovariateIds)
      
      if (length(covariateIdsToRemove) > 0) {
        covariateData$covariatesFiltered <-
          covariateData$covariatesFiltered |>
          dplyr::filter(!.data$covariateId %in% covariateIdsToRemove)
      }
    }
  }
  
  # Extract population size from metadata
  populationSize <-
    attr(x = covariateData, which = "metaData")$populationSize
  
  # Process time reference for readability
  timeRef <- covariateData$timeRef |>
    dplyr::collect() |>
    dplyr::mutate(timeLabel = paste0(.data$startDay, "d to ", .data$endDay, "d")) |>
    dplyr::select(.data$timeId, .data$timeLabel) |>
    dplyr::ungroup()
  
  # Collect analysis reference
  analysisRef <- covariateData$analysisRef |>
    dplyr::collect() |>
    dplyr::ungroup()
  
  # Process cohort reference for readability
  covariateRef <- covariateData$covariateRef |>
    dplyr::collect() |>
    dplyr::mutate(covariateName = stringr::str_extract(.data$covariateName, "(?<=: )\\w+")) |>
    dplyr::mutate(cohortId = (.data$covariateId - .data$analysisId) / 1000) |>
    dplyr::select(.data$cohortId, .data$covariateId, .data$covariateName) |>
    dplyr::rename(cohortName = .data$covariateName) |>
    dplyr::distinct() |>
    dplyr::arrange(.data$cohortId) |>
    dplyr::inner_join(
      eventCovariateIdsOfInterest |>
        dplyr::select(
          .data$covariateId,
          .data$newCovariateId,
          .data$newCovariateName
        ) |>
        dplyr::distinct(),
      by = "covariateId"
    )
  
  # Filter cohorts if covariate ID group is specified
  if (!is.null(flagCovariateIds)) {
    flagCovariateIds <-
      dplyr::tibble(
        covariateId = convertCohortIdToCovariateId(
          cohortIds = flagCovariateIds,
          cohortCovariateAnalysisId = analysisRef$analysisId
        ),
        flag = "1"
      )
    
    covariateRef <- covariateRef |>
      dplyr::left_join(flagCovariateIds,
                       by = "covariateId") |>
      tidyr::replace_na(replace = list(flag = "0"))
  } else {
    covariateRef <- covariateRef |>
      dplyr::mutate(flag = "1")
  }
  
  # Generate combinations of covariates for each person and time
  covariateCoOccurrenceData <- covariateData$covariatesFiltered |>
    dplyr::select(.data$rowId, .data$covariateId, .data$timeId) |>
    dplyr::rename(id = .data$rowId) |>
    dplyr::collect() |>
    dplyr::inner_join(covariateRef,
                      by = "covariateId") |>
    dplyr::select(.data$id,
                  .data$timeId,
                  .data$newCovariateId,
                  .data$flag) |>
    dplyr::group_by(.data$id, .data$timeId, .data$newCovariateId) |>
    dplyr::summarise(flag = max(.data$flag), .groups = "keep") |>
    dplyr::ungroup() |>
    dplyr::arrange(.data$id, .data$timeId, .data$newCovariateId) |>
    dplyr::group_by(.data$id, .data$timeId) |>
    dplyr::summarise(
      covariateCombinationId = paste(sort(.data$newCovariateId), collapse = ", "),
      flag = max(.data$flag)
    ) |>
    dplyr::ungroup() |>
    dplyr::arrange(.data$id, .data$timeId)
  
  # Aggregate and identify rare covariate combinations
  covariateCoOccurrenceRef <- covariateCoOccurrenceData |>
    dplyr::group_by(.data$timeId, .data$covariateCombinationId) |>
    dplyr::summarise(count = dplyr::n_distinct(.data$id)) |>
    dplyr::arrange(dplyr::desc(.data$count)) |>
    dplyr::mutate(rn = dplyr::row_number()) |>
    dplyr::mutate(isRare = dplyr::if_else(
      condition = (.data$rn <= (maxFeatures - 1)),
      true = 0,
      false = 1
    )) |>
    dplyr::select(.data$timeId,
                  .data$covariateCombinationId,
                  .data$isRare,
                  .data$count) |>
    dplyr::distinct()
  
  # Function to replace ID with covariate name
  replaceWithCovariateName <- function(id) {
    covariateRefToMatch <- covariateRef |>
      dplyr::select(.data$newCovariateId,
                    .data$newCovariateName) |>
      dplyr::distinct() |>
      dplyr::arrange(.data$newCovariateId)
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
      dplyr::arrange(.data$id,
                     .data$timeId,
                     .data$covariateCombinationId)
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
            .data$timeId,
            .data$covariateCombinationId,
            .data$covariateCombinationReduced
          ) |>
          dplyr::rename(covariateCombination = .data$covariateCombinationReduced),
        by = c("timeId",
               "covariateCombinationId")
      ) |>
      dplyr::arrange(.data$id,
                     .data$timeId) |>
      tidyr::pivot_wider(
        id_cols = c(id),
        names_prefix = "node",
        names_from = .data$timeId,
        values_from = .data$covariateCombination
      ) |>
      dplyr::group_by(dplyr::across(dplyr::starts_with("node"))) |> # Group by columns starting with "level"
      dplyr::summarize(size = dplyr::n(), .groups = "keep") |>
      dplyr::ungroup() |>
      dplyr::arrange(dplyr::desc(.data$size)) |>
      d3r::d3_nest(value_cols = "size")
  } else {
    result$sunburstRData <- covariateCoOccurrenceData |>
      dplyr::inner_join(
        result$covariateCoOccurrenceRef |>
          dplyr::select(
            .data$timeId,
            .data$covariateCombinationId,
            .data$covariateCombination
          ),
        by = c("timeId",
               "covariateCombinationId")
      ) |>
      dplyr::arrange(.data$id,
                     .data$timeId) |>
      tidyr::pivot_wider(
        id_cols = c(id),
        names_prefix = "node",
        names_from = .data$timeId,
        values_from = .data$covariateCombination
      ) |>
      dplyr::group_by(dplyr::across(dplyr::starts_with("node"))) |> # Group by columns starting with "level"
      dplyr::summarize(size = dplyr::n(), .groups = "keep") |>
      dplyr::ungroup() |>
      dplyr::arrange(dplyr::desc(.data$size)) |>
      d3r::d3_nest(value_cols = "size")
  }
  
  if (reduced) {
    ggAlluvialLodeForm <-
      covariateCoOccurrenceData |>
      dplyr::inner_join(
        result$covariateCoOccurrenceRef |>
          dplyr::select(
            .data$timeId,
            .data$covariateCombinationId,
            .data$covariateCombinationReduced
          ) |>
          dplyr::rename(covariateCombination = .data$covariateCombinationReduced),
        by = c("timeId",
               "covariateCombinationId")
      ) |>
      dplyr::arrange(.data$id,
                     .data$timeId) |>
      tidyr::pivot_wider(
        id_cols = c(id, flag),
        names_prefix = "node",
        names_from = .data$timeId,
        values_from = .data$covariateCombination
      ) |>
      dplyr::group_by(.data$flag, dplyr::across(dplyr::starts_with("node"))) |> # Group by columns starting with "level"
      dplyr::summarize(size = dplyr::n(), .groups = "keep") |>
      dplyr::ungroup() |>
      dplyr::arrange(.data$flag, dplyr::desc(.data$size))
  } else {
    ggAlluvialLodeForm <-
      covariateCoOccurrenceData |>
      dplyr::inner_join(
        result$covariateCoOccurrenceRef |>
          dplyr::select(
            .data$timeId,
            .data$covariateCombinationId,
            .data$covariateCombination
          ),
        by = c("timeId",
               "covariateCombinationId")
      ) |>
      dplyr::arrange(.data$id,
                     .data$timeId) |>
      tidyr::pivot_wider(
        id_cols = c(id, flag),
        names_prefix = "node",
        names_from = .data$timeId,
        values_from = .data$covariateCombination
      ) |>
      dplyr::group_by(.data$flag, dplyr::across(dplyr::starts_with("node"))) |> # Group by columns starting with "level"
      dplyr::summarize(size = dplyr::n(), .groups = "keep") |>
      dplyr::ungroup() |>
      dplyr::arrange(.data$flag, dplyr::desc(.data$size))
  }
  
  colPositions <- which(grepl("node", names(ggAlluvialLodeForm)))
  
  result$ggAlluvialLodeForm <- ggAlluvialLodeForm |>
    ggalluvial::to_lodes_form(key = "state", axes = colPositions)
  
  return(result)
}
