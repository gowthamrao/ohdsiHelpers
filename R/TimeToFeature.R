#' @export
timeToFeature <- function(targetCohortId,
                          featureCohortId,
                          allDatabaseIds,
                          cohortTableName = cohortTableNames$cohortTable,
                          cdmSources,
                          plotTitle = NULL,
                          minDays = NULL,
                          maxDays = NULL,
                          cohortCounts,
                          cohortDefinitionSet) {
  timeToFeatureCohort <-
    OhdsiHelpers::getTargetCohortToFeatureCohortDateDifferenceDistributionInParrallel(
      cdmSources = cdmSources,
      targetCohortTableName = cohortTableNames$cohortTable,
      targetCohortIds = targetCohortId,
      featureCohortIds = featureCohortId,
      databaseIds = allDatabaseIds,
      minDays = minDays,
      maxDays = maxDays
    )
  
  if (is.null(plotTitle)) {
    plotTitle <- paste0(
      "Time to first ",
      if (!is.null(maxDays)) {
        paste0(" ( within ", maxDays, ") ")
      },
      cohortDefinitionSet |>
        dplyr::filter(cohortId == featureCohortId) |>
        dplyr::mutate(goodName = OhdsiHelpers::createGoodCohortNames(cohortNameShort)) |>
        dplyr::pull(goodName)
    )
  }
  
  preparedData <-
    timeToFeatureCohort  |>
    OhdsiHelpers::prepareTargetCohortToFeatureCohortDateDifferenceDistributionForVisualizations()
  
  summaryStatistics <-
    preparedData |>
    OhdsiHelpers::calculateSummaryStatistics() |>
    dplyr::mutate(value = OhdsiHelpers::formatDecimalWithComma(number = value)) |>
    tidyr::pivot_wider(id_cols = "statistic",
                       values_from = value,
                       names_from = group) |>
    dplyr::arrange(statistic)
  
  violinPlots <-
    preparedData |>
    OhdsiPlots::createViolinPlot(title = plotTitle)
  
  
  
  output <- c()
  
  if (length(featureCohortId) == 1) {
    cohortCounts <- cohortCounts$cohortCounts |>
      dplyr::filter(cohortId %in% c(featureCohortId,
                                    targetCohortId)) |>
      dplyr::select(cohortId,
                    databaseId,
                    cohortSubjects)
    
    output$featureProportion <- cohortCounts |>
      dplyr::filter(cohortId == featureCohortId) |>
      dplyr::select(databaseId,
                    cohortSubjects) |>
      dplyr::inner_join(
        cohortCounts |>
          dplyr::filter(cohortId == targetCohortId) |>
          dplyr::select(databaseId,
                        cohortSubjects) |>
          dplyr::rename(targetSubjects = cohortSubjects),
        by = "databaseId"
      ) |>
      dplyr::mutate(proportion = cohortSubjects / targetSubjects) |>
      dplyr::mutate(report = OhdsiHelpers::formatCountPercent(count = cohortSubjects, percent = proportion)) |>
      dplyr::select(databaseId,
                    report) |>
      tidyr::pivot_wider(names_from = databaseId,
                         values_from = report)
  }
  
  output$summaryStatistics <- summaryStatistics
  output$violinPlot <- violinPlots
  return(output)
}