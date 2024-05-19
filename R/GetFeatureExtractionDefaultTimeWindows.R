#' @export
getFeatureExtractionDefaultTimeWindows <-
  function(cummulative = NULL,
           periodTypes = NULL) {
    filePath <-
      system.file("FeatureExtractionTimeWindows.csv", package = "OhdsiHelpers")
    timeWindows <-
      readr::read_csv(file = filePath, col_types = readr::cols())
    
    if (!is.null(cummulative)) {
      timeWindows <- cummulative |>
        dplyr::filter(sequenceCummulative == TRUE)
    }
    
    if (!is.null(periodTypes)) {
      timeWindows <- cummulative |>
        dplyr::filter(period %in% c(periodTypes))
    }
    return(timeWindows)
  }
