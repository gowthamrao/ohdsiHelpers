# Function to compare two lists and return a tibble
#' @export
createFeatureExtractionWideReportTemporalCovariateData <-
  function(covariateData,
           cohortId,
           timeIds = NULL) {
    if (!FeatureExtraction::isTemporalCovariateData(covariateData)) {
      stop("Covariate data is not temporal covariate data")
    }
    
    report <- covariateData$covariates |>
      dplyr::filter(cohortDefinitionId == cohortId) |>
      dplyr::select(covariateId,
                    timeId,
                    sumValue,
                    averageValue) |>
      dplyr::collect() |>
      dplyr::arrange(timeId, dplyr::desc(averageValue)) |>
      dplyr::mutate(report = OhdsiHelpers::formatCountPercent(count = sumValue, percent = averageValue)) |>
      dplyr::select(-sumValue, -averageValue)
    
    if (!is.null(timeIds)) {
      report <- report |>
        dplyr::filter(timeId %in% c(NA, timeIds))
    }
    
    report <- dplyr::bind_rows(report |>
      dplyr::inner_join(
        covariateData$timeRef |>
          dplyr::collect() |>
          dplyr::arrange(timeId) |>
          dplyr::mutate(timeName = paste0("t", startDay, "d to t", endDay, "d"))
      ),
      report |> 
        dplyr::filter(is.na(timeId)) |> 
        dplyr::mutate(timeName = "t_invariant")
      ) |>
      dplyr::inner_join(
        covariateData$covariateRef |>
          dplyr::select(covariateId,
                        covariateName) |>
          dplyr::collect() |>
          dplyr::mutate(
            covariateName = stringr::str_replace(covariateName, "^[^:]*:\\s*", "")
          ) |>
          dplyr::mutate(
            covariateName = stringr::str_replace(covariateName, "\\[.*?\\]", "")
          ),
        by = "covariateId"
      ) |>
      dplyr::relocate(covariateName) |>
      tidyr::pivot_wider(
        id_cols = c(covariateId, covariateName),
        names_from = timeName,
        values_from = report
      )
    
    return(report)
  }
