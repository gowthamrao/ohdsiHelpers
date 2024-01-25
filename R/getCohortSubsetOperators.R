#' Create Cohort Subset Operators
#'
#' This function generates a list of cohort subsets based on specified time windows,
#' cohort IDs, combination operators, and a base name. It is designed to facilitate
#' cohort analysis by allowing for the creation of various time-bound cohort subsets.
#'
#' @param subsetCohortIds Vector of cohort IDs for subset creation.
#' @param subsetCombinationOperator Operator for combining subsets (e.g., "AND", "OR").
#' @param baseName Base name for the generated cohort subsets.
#' @param timeStart Start day for the time window (default: -9999).
#' @param timeEnd End day for the time window (default: 9999).
#' @param targetAnchor Anchor point for the time window (default: "cohortStart").
#'
#' @return A list of cohort subset objects.
#' @export
#'
#' @examples
#' # Example usage:
#' results <- getCohortSubsetOperators(subsetCohortIds = c(1, 2, 3),
#'                                     subsetCombinationOperator = "AND",
#'                                     baseName = "exampleCohort")
getCohortSubsetOperators <- function(subsetCohortIds,
                                     subsetCombinationOperator,
                                     baseName,
                                     timeStart = -9999,
                                     timeEnd = 9999,
                                     targetAnchor = "cohortStart") {
  if (targetAnchor == "cohortStart") {
    targetAnchorName = "Starts"
  } else if (targetAnchor == "cohortEnd") {
    targetAnchorName = "Ends"
  } else {
    stop("targetAnchor should be either cohortStart or cohortEnd.")
  }
  
  # Validate input parameters
  if (timeStart > timeEnd) {
    stop("timeStart > timeEnd")
  }
  if (!is.vector(subsetCohortIds)) {
    stop("subsetCohortIds must be a vector.")
  }
  if (!is.character(subsetCombinationOperator)) {
    stop("subsetCombinationOperator must be a character string.")
  }
  if (!is.character(baseName)) {
    stop("baseName must be a character string.")
  }
  
  # Helper function to create subset cohort window
  createWindow <- function(startDay, endDay) {
    tryCatch({
      CohortGenerator::createSubsetCohortWindow(startDay = startDay,
                                                endDay = endDay,
                                                targetAnchor = targetAnchor)
    }, error = function(e) {
      stop("Error in creating subset cohort window: ", e$message)
    })
  }
  
  # Define cohort subset windows
  anytimeBeforeToAnytimeAfter <- createWindow(timeStart, timeEnd)
  anytimeBeforeTo1DayBefore <- createWindow(timeStart,-1)
  anyTimeBeforeToDay0 <- createWindow(timeStart, 0)
  onDay0 <- createWindow(0, 0)
  onDay0ToAnytimeAfter <- createWindow(0, timeEnd)
  day1ToAnytimeAfter <- createWindow(1, timeEnd)
  
  windowVars <- list(
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
        paste0("limitWhenSubsetCohort_",
               targetAnchorName,
               "_",
               period,
               suffix)
      tryCatch({
        results[[objectName]] <- CohortGenerator::createCohortSubset(
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
      }, error = function(e) {
        stop("Error in creating cohort subset: ", e$message)
      })
    }
  }
  
  return(results)
}
