#' @export
getCohortSubsetOperators <-
  function(subsetCohortIds,
           subsetCombinationOperator,
           baseName,
           timeStart = -9999,
           timeEnd = 9999,
           targetAnchor = "cohortStart") {
    # Helper function to create subset cohort window
    createWindow <- function(startDay, endDay) {
      CohortGenerator::createSubsetCohortWindow(startDay = startDay,
                                                endDay = endDay,
                                                targetAnchor = targetAnchor)
    }
    
    # Define cohort subset windows
    anytimeBeforeToAnytimeAfter <-
      createWindow(timeStart, timeEnd)
    anytimeBeforeTo1DayBefore <- createWindow(timeStart,-1)
    anyTimeBeforeToDay0 <- createWindow(timeStart, 0)
    onDay0 <- createWindow(0, 0)
    onDay0ToAnytimeAfter <- createWindow(0, timeEnd)
    day1ToAnytimeAfter <- createWindow(1, timeEnd)
    
    windowVars <-
      list(
        Before = anytimeBeforeTo1DayBefore,
        OnOrBefore = anyTimeBeforeToDay0,
        On = onDay0,
        OnOrAfter = onDay0ToAnytimeAfter,
        After = day1ToAnytimeAfter
      )
    
    # Initialize a list to store results
    results <- list()
    
    # Create and assign cohort subsets to the results list
    for (period in c("Before", "OnOrBefore", "On", "OnOrAfter", "After")) {
      for (negate in c(FALSE, TRUE)) {
        suffix <- ifelse(negate, "_Negate", "")
        windowVar <- windowVars[[period]]
        objectName <-
          paste0("limitWhenSubsetCohort_Starts_",
                 period,
                 "_",
                 baseName,
                 suffix)
        results[[objectName]] <-
          CohortGenerator::createCohortSubset(
            name = paste0(
              "with ",
              baseName,
              " ",
              tolower(period),
              ifelse(negate, "out", "")
            ),
            cohortIds = subsetCohortIds,
            cohortCombinationOperator = subsetCombinationOperator,
            negate = negate,
            startWindow = windowVar,
            endWindow = anytimeBeforeToAnytimeAfter
          )
      }
    }
    return(results)
  }
