#' @export
prepareTargetCohortToFeatureCohortDateDifferenceDistributionForHistogram <-
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
    
    browser()
    output <- output |>
      tidyr::uncount(weights = subjects, .remove = FALSE) |>
      dplyr::mutate(myaxis = paste0(databaseId, "\n", "n=", subjectsTotal)) |>
      dplyr::rename(value = daysToNextCohortStart,
                    name = databaseId) |>
      dplyr::select(value, name, myaxis)
    
    return(output)
  }