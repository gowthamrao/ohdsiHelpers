#' @export
getFeatureExtractionReportCommonSequentialTimePeriods <-
  function() {
    # covariateData$timeRef |>
    #   dplyr::collect() |>
    #   dplyr::filter(endDay == -1) |>
    #   dplyr::filter(startDay %% 2 != 0) |>
    #   dplyr::filter(startDay > -400) |>
    #   dplyr::filter(startDay != -365)
    #
    # covariateData$timeRef |>
    #   dplyr::collect() |>
    #   dplyr::filter(startDay == 1) |>
    #   dplyr::filter(endDay %% 2 != 0) |>
    #   dplyr::filter(endDay != 365) |>
    #   dplyr::filter(endDay < 400)
    
    
    priorMonthlyPeriods <- dplyr::tibble(
      timeId = c(15, 21, 23, 25, 27, 29, 31, 33, 36, 38, 41, 44, 47),
      startDay = c(
        -391,-361,-331,-301,-271,-241,-211,-181,-151,-121,-91,-61,-31
      ),
      endDay = c(-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1)
    )
    
    postMonthlyPeriods <- dplyr::tibble(
      timeId = c(58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70),
      startDay = c(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1),
      endDay = c(1, 31, 61, 91, 121, 151, 181, 211, 241, 271, 301, 331, 361)
    )
    
    onDayOf <- dplyr::tibble(timeId = 53,
                             startDay = 0,
                             endDay = 0)
    
    timePeriods <- dplyr::bind_rows(priorMonthlyPeriods,
                                    postMonthlyPeriods,
                                    onDayOf) |>
      dplyr::arrange(timeId)
    
    return(timePeriods)
    
  }