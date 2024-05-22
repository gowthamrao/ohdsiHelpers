#' Extract default time windows for feature extraction
#'
#' This function reads a CSV file containing time windows for feature extraction and filters
#' the time windows based on the cumulative and period type parameters.
#'
#' @param cummulative A logical value indicating whether cumulative time windows should be returned.
#'                    Can be TRUE, FALSE, or NULL (default), where NULL returns all records.
#' @param periodTypes A character vector specifying the types of periods to filter by.
#'                    Can be "month", "year", or NULL (default), where NULL returns all records.
#' @return A data frame containing the filtered time windows.
#' @export
getFeatureExtractionDefaultTimeWindows <-
  function(cummulative = NULL,
           periodTypes = NULL,
           selectedCummulative = NULL) {
    # Path to the CSV file within the OhdsiHelpers package
    filePath <-
      system.file("FeatureExtractionTimeWindows.csv", package = "OhdsiHelpers")
    
    # Reading the CSV file into a data frame
    timeWindows <-
      readr::read_csv(file = filePath, col_types = readr::cols())
    
    # Assert checks for `cummulative` to be TRUE, FALSE, or NULL
    assertthat::assert_that(is.logical(cummulative) ||
                              is.null(cummulative),
                            msg = "cummulative must be TRUE, FALSE, or NULL.")
    
    # Filter for cumulative time windows if `cummulative` is not NULL
    if (!is.null(cummulative)) {
      timeWindows <-
        timeWindows[timeWindows$sequenceCummulative == cummulative, ]
    }
    
    # Assert checks for `periodTypes` to be either "month", "year", or NULL
    validPeriods <- c("month", "year")
    if (!is.null(periodTypes)) {
      assertthat::assert_that(all(periodTypes %in% validPeriods),
                              msg = "periodTypes must be 'month', 'year', or NULL.")
      timeWindows <-
        timeWindows[timeWindows$period %in% periodTypes, ]
    }
    
    timeWindows <- timeWindows |>
      dplyr::mutate(periodName = paste0("d",
                                        startDay |> as.integer(),
                                        "d",
                                        endDay |> as.integer()))
    
    if (isTRUE(selectedCummulative)) {
      filteredDataFrame <- timeWindows |>
        dplyr::select(startDay,
                      endDay) |>
        dplyr::filter(
          startDay %in% c(-391, -301, -181, -91, -31) |
            endDay %in% c(0, 31, 91, 181, 241, 361) |
            (startDay == 0 & endDay == 0)
        ) |>
        dplyr::mutate(selectedCummulative = 1) |>
        dplyr::distinct()
      
      timeWindows <- timeWindows |>
        dplyr::inner_join(filteredDataFrame,
                          by = c("startDay", "endDay"))
    }
    
    timeWindows <- timeWindows |>
      dplyr::select(startDay,
                    endDay,
                    periodName,
                    windowType) |>
      dplyr::distinct()
    
    
    return(timeWindows)
  }
