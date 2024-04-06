#' Prepare Data for Violin Plots
#'
#' This function prepares event distribution data specifically for violin plot visualization. It filters and transforms the data based on the provided criteria related to cohort start sequences and the number of days to the next cohort start.
#'
#' @param data A data frame containing the columns `startSequence`, `daysToNextCohortStart`, `subjects`, `databaseId`, and `subjectsTotal`. This is the dataset to be prepared for the violin plot.
#' @param startSequence An integer indicating the start sequence to filter the data by. If `-1`, no filtering on start sequence will be applied. Default is `-1`.
#' @param minDaysToNextCohortStart An optional integer specifying the minimum number of days to the next cohort start for filtering the data. Rows with `daysToNextCohortStart` less than this value will be excluded. If `NULL`, no minimum limit will be applied.
#' @param maxDaysToNextCohortStart An optional integer specifying the maximum number of days to the next cohort start for filtering the data. Rows with `daysToNextCohortStart` greater than this value will be excluded. If `NULL`, no maximum limit will be applied.
#'
#' @return A data frame prepared for violin plot visualization with columns `value` (formerly `daysToNextCohortStart`), `name` (formerly `databaseId`), and `myaxis` (a concatenation of `databaseId` and `subjectsTotal` for labeling).
#' @export
#' @examples
#' preparedData <- prepareEventDistributionDataForViolinPlots(myData, -1, NULL, NULL)

#' @export
prepareTargetCohortToFeatureCohortDateDifferenceDistributionForViolinPlots <-
  function(data,
           startSequence = NULL,
           minDaysToNextCohortStart = NULL,
           maxDaysToNextCohortStart = NULL) {
    # Assert checks
    assertthat::assert_that(is.data.frame(data), msg = "Data must be a data frame.")
    
    if (!is.null(startSequence)) {
      assertthat::assert_that(is.numeric(startSequence), msg = "Start sequence must be numeric.")
    }
    
    if (!is.null(minDaysToNextCohortStart)) {
      assertthat::assert_that(is.numeric(minDaysToNextCohortStart), msg = "Minimum days to next cohort start must be numeric.")
    }
    if (!is.null(maxDaysToNextCohortStart)) {
      assertthat::assert_that(is.numeric(maxDaysToNextCohortStart), msg = "Maximum days to next cohort start must be numeric.")
    }
    
    output <- data
    
    if (!is.null(startSequence)) {
      output <- output |>
        dplyr::filter(startSequence %in% c(!!startSequence))
    }
    if (!is.null(minDaysToNextCohortStart)) {
      output <- output |>
        dplyr::filter(daysToNextCohortStart >= minDaysToNextCohortStart)
    }
    if (!is.null(maxDaysToNextCohortStart)) {
      output <- output |>
        dplyr::filter(daysToNextCohortStart <= maxDaysToNextCohortStart)
    }
    
    output <- output |>
      tidyr::uncount(weights = subjects, .remove = FALSE) |>
      dplyr::mutate(myaxis = paste0(databaseId, "\n", "n=", subjectsTotal)) |>
      dplyr::rename(value = daysToNextCohortStart,
                    name = databaseId) |>
      dplyr::select(value, name, myaxis)
    
    return(output)
  }