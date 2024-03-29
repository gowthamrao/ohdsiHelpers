#' @export
createFeatureExtractionReportByTimeWindows <-
  function(covariateData,
           startDays,
           endDays,
           minAverageValue = 0.01,
           cohortIdToStudy,
           cohortDefinitionSet,
           databaseId,
           database,
           cohortName = NULL,
           reportName = NULL) {
    report <- covariateData$covariates |>
      dplyr::filter(cohortDefinitionId == cohortIdToStudy) |>
      dplyr::inner_join(
        covariateData$timeRef |>
          dplyr::filter(startDay %in% startDays,
                        endDay %in% endDays) |>
          dplyr::mutate(
            periodName = paste0("d",
                                startDay |> as.integer(),
                                "d",
                                endDay |> as.integer())
          )
      ) |>
      dplyr::inner_join(covariateData$covariateRef) |>
      dplyr::inner_join(covariateData$analysisRef) |>
      dplyr::arrange(startDay,
                     endDay,
                     dplyr::desc(averageValue)) |>
      dplyr::select(
        covariateId,
        covariateName,
        conceptId,
        analysisId,
        analysisName,
        domainId,
        periodName,
        sumValue,
        averageValue
      ) |>
      dplyr::filter(averageValue > minAverageValue) |>
      dplyr::collect() |>
      dplyr::mutate(metric = OhdsiHelpers::formatCountPercent(count = sumValue, percent = averageValue)) |>
      tidyr::pivot_wider(
        id_cols = c(
          covariateId,
          covariateName,
          conceptId,
          analysisId,
          analysisName,
          domainId
        ),
        names_from = periodName,
        values_from = metric
      ) |>
      dplyr::mutate(conceptId = (covariateId - analysisId) / 1000)
    
    if (!is.null(cohortName)) {
      report <-
        dplyr::bind_rows(report |>
                           dplyr::slice(0),
                         dplyr::tibble(covariateName = cohortName),
                         report)
    }
    
    report <- dplyr::bind_rows(report |>
                                 dplyr::slice(0),
                               dplyr::tibble(covariateName = databaseId),
                               report)
    if (!is.null(cohortName)) {
      report <-
        dplyr::bind_rows(report |>
                           dplyr::slice(0),
                         dplyr::tibble(covariateName = cohortName),
                         report)
    }
    
    if (!is.null(reportName)) {
      report <-
        dplyr::bind_rows(report |>
                           dplyr::slice(0),
                         dplyr::tibble(covariateName = reportName),
                         report)
    }
    
    return(report)
  }