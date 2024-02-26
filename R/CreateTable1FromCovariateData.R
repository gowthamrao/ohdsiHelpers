#' @export
checkFilterTemporalCovariateDataByTimeIdCohortId <-
  function(covariateData,
           cohortId,
           timeId,
           multipleCohortId) {
    if (FeatureExtraction::isTemporalCovariateData(covariateData)) {
      if (is.null(timeId)) {
        stop("one of given covariateData is temporal. its corresponding timeId is NULL")
      } else if (length(timeId) != 1) {
        stop("timeId can only be of length 1")
      } else {
        covariateData$timeRef <- covariateData$timeRef |>
          dplyr::filter(timeId == !!timeId)
        covariateData$covariates <- covariateData$covariates |>
          dplyr::filter(timeId == !!timeId)
      }
    }

    if (is.null(cohortId)) {
      if (length(attributes(covariateData)$metaData$cohortIds) > 1) {
        stop(
          "covariateData has records for more than one cohortId. Please provide cohortId to filter"
        )
      }
    } else if (length(cohortId) != 1) {
      if (multipleCohortId) {
        warning("experimental support for multiple cohort id")
      } else {
        stop("cohortId has a length more than 1")
      }
    } else {
      covariateData$covariates <- covariateData$covariates |>
        dplyr::filter(.data$cohortDefinitionId == !!cohortId)
    }
    return(covariateData)
  }




#' @export
createTable1SpecificationsRow <- function(analysisId,
                                          conceptIds = NULL,
                                          covariateIds = NULL,
                                          label = "Feature cohorts") {
  if (is.null(conceptIds) & is.null(covariateIds)) {
    stop("please provide atleast conceptIds or covariateIds")
  }

  if (!length(analysisId) == 1) {
    stop("only one analysis id")
  }

  if (!length(label) == 1) {
    stop("only one label")
  }

  covariateIds <- c(
    covariateIds,
    (conceptIds * 1000) + analysisId
  ) |>
    unique()

  output <- dplyr::tibble(
    label = label,
    analysisId = analysisId,
    covariateIds = paste0(covariateIds, collapse = ",")
  )
  return(output)
}


#' @export
CreateTable1FromCovariateData <- function(covariateData1,
                                          covariateData2 = NULL,
                                          cohortId1 = NULL,
                                          cohortId2 = NULL,
                                          multipleCohortId = FALSE,
                                          analysisName = NULL,
                                          analysisId = NULL,
                                          timeId1 = NULL,
                                          timeId2 = NULL,
                                          table1Specifications = NULL,
                                          output = "two columns",
                                          showCounts = FALSE,
                                          showPercent = TRUE,
                                          percentDigits = 1,
                                          valueDigits = 1,
                                          stdDiffDigits = 2,
                                          rangeHighPercent = 1,
                                          rangeLowPercent = 0.01) {
  covariateData1 <-
    checkFilterTemporalCovariateDataByTimeIdCohortId(
      covariateData = covariateData1,
      cohortId = cohortId1,
      timeId = timeId1,
      multipleCohortId = multipleCohortId
    )
  populationSizeCovariateData1 <-
    attributes(covariateData1)$metaData$populationSize |> as.numeric()

  if (!is.null(covariateData2)) {
    covariateData2 <-
      checkFilterTemporalCovariateDataByTimeIdCohortId(
        covariateData = covariateData2,
        cohortId = cohortId2,
        timeId = timeId2,
        multipleCohortId = multipleCohortId
      )
    populationSizeCovariateData2 <-
      attributes(covariateData2)$metaData$populationSize |> as.numeric()
  }

  processCovariateData <- function(covariateData,
                                   rangeLowPercent,
                                   rangeHighPercent) {
    filteringCovariateIdsThatHaveMinThreshold <-
      covariateData$covariates |>
      dplyr::filter(.data$averageValue >= !!rangeLowPercent) |>
      dplyr::filter(.data$averageValue <= !!rangeHighPercent) |>
      dplyr::select(
        "covariateId",
        "averageValue"
      ) |>
      dplyr::arrange(dplyr::desc(.data$averageValue)) |>
      dplyr::mutate(rn = dplyr::row_number()) |>
      dplyr::select("covariateId", "rn") |>
      dplyr::distinct() |>
      dplyr::inner_join(
        covariateData$covariateRef |>
          dplyr::select(
            "covariateId",
            "analysisId",
            "conceptId"
          ) |>
          dplyr::distinct(),
        by = "covariateId"
      ) |>
      dplyr::inner_join(
        covariateData$analysisRef |>
          dplyr::select(
            "analysisId",
            "analysisName"
          ) |>
          dplyr::distinct() #|> dplyr::collect()
        ,
        by = "analysisId"
      ) |>
      dplyr::arrange(.data$rn) |>
      dplyr::collect()

    return(filteringCovariateIdsThatHaveMinThreshold)
  }


  if (is.null(table1Specifications)) {
    filteringCovariateIdsThatHaveMinThreshold <-
      processCovariateData(
        covariateData = covariateData1,
        rangeLowPercent = rangeLowPercent,
        rangeHighPercent = rangeHighPercent
      )
    if (!is.null(covariateData2)) {
      filteringCovariateIdsThatHaveMinThreshold <- dplyr::bind_rows(
        filteringCovariateIdsThatHaveMinThreshold,
        processCovariateData(
          covariateData = covariateData2,
          rangeLowPercent = rangeLowPercent,
          rangeHighPercent = rangeHighPercent
        )
      )
    }

    table1AnalysisSpecifications <- c()

    if (!is.null(analysisName)) {
      filteringCovariateIdsThatHaveMinThreshold <-
        filteringCovariateIdsThatHaveMinThreshold |>
        dplyr::filter(analysisName %in% !!analysisName)
    }

    if (!is.null(analysisId)) {
      filteringCovariateIdsThatHaveMinThreshold <-
        filteringCovariateIdsThatHaveMinThreshold |>
        dplyr::filter(analysisId %in% !!analysisId)
    }

    analysisNames <- filteringCovariateIdsThatHaveMinThreshold |>
      dplyr::select(analysisName) |>
      dplyr::distinct() |>
      dplyr::arrange() |>
      dplyr::collect() |>
      dplyr::pull(analysisName)

    for (i in (1:length(analysisNames))) {
      analysisName <-
        analysisNames[[i]]
      covariateIds <- filteringCovariateIdsThatHaveMinThreshold |>
        dplyr::filter(analysisName %in% !!analysisName) |>
        dplyr::select(.data$covariateId) |>
        dplyr::distinct() |>
        dplyr::collect() |>
        dplyr::pull(.data$covariateId) # dont sort

      analysisIds <- filteringCovariateIdsThatHaveMinThreshold |>
        dplyr::filter(analysisName %in% !!analysisName) |>
        dplyr::select(analysisId) |>
        dplyr::distinct() |>
        dplyr::collect() |>
        dplyr::pull(analysisId) |>
        sort()

      if (length(analysisIds) != 1) {
        stop("Please check covariateData. More than one analysisId for the same analysisName")
      }

      table1AnalysisSpecifications[[i]] <-
        createTable1SpecificationsRow(
          analysisId = analysisIds,
          conceptIds = NULL,
          covariateIds = covariateIds,
          label = analysisName |> SqlRender::camelCaseToTitleCase() |> stringr::str_trim() |> stringr::str_squish()
        )
    }
    table1AnalysisSpecifications <-
      dplyr::bind_rows(table1AnalysisSpecifications)
  }

  report <- FeatureExtraction::createTable1(
    covariateData1 = covariateData1,
    covariateData2 = covariateData2,
    cohortId1 = cohortId1,
    cohortId2 = cohortId2,
    specifications = table1AnalysisSpecifications,
    output = output,
    showCounts = showCounts,
    showPercent = showPercent,
    percentDigits = percentDigits,
    valueDigits = valueDigits,
    stdDiffDigits = stdDiffDigits
  )

  return(report)
}
