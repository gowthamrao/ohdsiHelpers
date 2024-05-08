#' @export
getSubsetOperatorsStandard <- function() {
  subsetOperators <- list()
  
  subsetOperators$limitFirstEver <-
    CohortGenerator::createLimitSubset(name = "|subset limit - first 365",
                                       limitTo = "firstEver")
  
  subsetOperators$limitPrior365d <-
    CohortGenerator::createLimitSubset(name = "|subset limit - prior 365 days",
                                       priorTime = 365)
  
  subsetOperators$limitFirstEver365d <-
    CohortGenerator::createLimitSubset(name = "|subset limit - first with prior 365 days",
                                       limitTo = "firstEver",
                                       priorTime = 365)
  
  subsetOperators$limitValidGenderAgeMin0 <-
    CohortGenerator::createDemographicSubset(name = "|subset demographics - valid biological sex",
                                             gender = c(8532, 8507),
                                             ageMin = 0)
  
  subsetOperators$limitValidAgeMin1 <-
    CohortGenerator::createDemographicSubset(name = "|subset demographics - age greater than 1",
                                             ageMin = 1)
  
  subsetOperators$limitValidAgeMax90 <-
    CohortGenerator::createDemographicSubset(name = "|subset demographics - age less than 90",
                                             ageMax = 90)
  
  subsetOperators$limitValidAgeMax99 <-
    CohortGenerator::createDemographicSubset(name = "|subset demographics - age less than 99",
                                             ageMax = 99)
  
  subsetOperators$limitValidAgeMax65 <-
    CohortGenerator::createDemographicSubset(name = "|subset demographics - age less than 65",
                                             ageMax = 65)
  
  return(subsetOperators)
}

#' @export
getSubsetOperatorEnM <- function(startDay = 0,
                                 endDay = 60,
                                 targetAnchor = "cohortStart",
                                 cohortIdLevel4or5New = 16174,
                                 cohortIdLevel4or5Established = 16176) {
  subsetOperators <- list()
  
  subsetOperators$subsetOperatorHasNewOrEstablishedEnM <-
    getCohortSubsetOperators(
      subsetCohortIds = c(cohortIdLevel4or5New,
                          cohortIdLevel4or5Established),
      cohortCombinationOperator = "any",
      baseName = "01 new or established level 4 E&M visit",
      startDay = startDay,
      endDay = endDay
    )
  
  subsetOperators$subsetOperatorHasNewEnM <-
    getCohortSubsetOperators(
      subsetCohortIds = c(cohortIdLevel4or5New),
      cohortCombinationOperator = "any",
      baseName = "02 new level 4 E&M visit",
      startDay = startDay,
      endDay = endDay
    )
  
  subsetOperators$subsetOperatorHasEstablishedEnM <-
    getCohortSubsetOperators(
      subsetCohortIds = c(cohortIdLevel4or5Established),
      cohortCombinationOperator = "any",
      baseName = "03 established level 4 E&M visit",
      startDay = startDay,
      endDay = endDay
    )
  
  subsetOperators$subsetOperatorHasNewAndEstablishedEnM <-
    getCohortSubsetOperators(
      subsetCohortIds = c(cohortIdLevel4or5New,
                          cohortIdLevel4or5Established),
      cohortCombinationOperator = "all",
      baseName = "04 new and established level 4 E&M visit",
      startDay = startDay,
      endDay = endDay
    )
  
  subsetOperators$subsetOperatorHasNewAndEstablishedEnMNegate <-
    getCohortSubsetOperators(
      subsetCohortIds = c(cohortIdLevel4or5New,
                          cohortIdLevel4or5Established),
      cohortCombinationOperator = "any",
      baseName = "05 new and established level 4 E&M visit",
      startDay = startDay,
      endDay = endDay,
      negate = TRUE
    )
  
  subsetOperators$subsetOperatorHasNewEnMNegate <-
    getCohortSubsetOperators(
      subsetCohortIds = c(cohortIdLevel4or5New),
      cohortCombinationOperator = "all",
      baseName = "06 new level 4 E&M visit",
      startDay = startDay,
      endDay = endDay,
      negate = TRUE
    )
  
  subsetOperators$subsetOperatorHasNewOrEstablishedEnMNegate <-
    getCohortSubsetOperators(
      subsetCohortIds = c(cohortIdLevel4or5New,
                          cohortIdLevel4or5Established),
      cohortCombinationOperator = "any",
      baseName = "07 new or established level 4 E&M visit",
      startDay = startDay,
      endDay = endDay,
      negate = TRUE
    )
  return(subsetOperators)
}

#' Create Cohort Subset Operators
#'
#' This function generates one cohort subset operator for a specified time window,
#' cohort Ids, cohort combination operators, and a base name. It allows consistent
#' naming of the subset names.
#'
#' @param subsetCohortIds Vector of cohort IDs for subset creation.
#' @param cohortCombinationOperator Operator for combining subsets (e.g., "AND", "OR").
#' @param baseName Base name for the generated cohort subsets.
#' @param startDay Start day for the time window (default: -9999).
#' @param endDay End day for the time window (default: 9999).
#' @param targetAnchor Anchor point for the time window (default: "cohortStart").
#' @param negate Negate the operator
#'
#' @return A list of cohort subset objects.
#' @export
#'
#' @examples
#' # Example usage:
#' results <- getCohortSubsetOperators(
#'   subsetCohortIds = c(1, 2, 3),
#'   cohortCombinationOperator = "any",
#'   baseName = "exampleCohort"
#' )
getCohortSubsetOperators <- function(subsetCohortIds,
                                     cohortCombinationOperator = "any",
                                     baseName,
                                     startDay = -9999,
                                     endDay = 9999,
                                     negate = FALSE,
                                     targetAnchor = "cohortStart") {
  if (targetAnchor == "cohortStart") {
    targetAnchorName <- "Starts"
  } else if (targetAnchor == "cohortEnd") {
    targetAnchorName <- "Ends"
  } else {
    stop("targetAnchor should be either cohortStart or cohortEnd.")
  }
  
  if (length(startDay) > 1) {
    stop("only one startDay time window allowed")
  }
  
  if (length(endDay) > 1) {
    stop("only one endDay time window allowed")
  }
  
  # Validate input parameters
  if (startDay > endDay) {
    stop("startDay > endDay")
  }
  if (!is.vector(subsetCohortIds)) {
    stop("subsetCohortIds must be a vector.")
  }
  if (!is.character(cohortCombinationOperator)) {
    stop("cohortCombinationOperator must be a character string.")
  }
  if (!cohortCombinationOperator %in% c("any", "all")) {
    stop("cohortCombinationOperator may only be any or all")
  }
  if (!is.character(baseName)) {
    stop("baseName must be a character string.")
  }
  
  
  if (targetAnchor == "cohortStart") {
    startWindow <-
      CohortGenerator::createSubsetCohortWindow(startDay = startDay,
                                                endDay = endDay,
                                                targetAnchor = targetAnchor)
    endWindow <-
      CohortGenerator::createSubsetCohortWindow(
        startDay = -9999,
        endDay = 9999,
        targetAnchor = targetAnchor
      )
    
  }
  
  if (targetAnchor == "cohortEnd") {
    startWindow <-
      CohortGenerator::createSubsetCohortWindow(
        startDay = -9999,
        endDay = 9999,
        targetAnchor = targetAnchor
      )
    endWindow <-
      CohortGenerator::createSubsetCohortWindow(startDay = startDay,
                                                endDay = endDay,
                                                targetAnchor = targetAnchor)
    
  }
  
  subsetOperatorName <- paste0("|subset cohort - ",
                               baseName,
                               " (",
                               startDay,
                               "d to ",
                               endDay,
                               "d)",
                               if (negate) {
                                 " NEGATE "
                               })
  
  subsetOperator <-
    CohortGenerator::createCohortSubset(
      name = subsetOperatorName,
      cohortIds = subsetCohortIds,
      cohortCombinationOperator = cohortCombinationOperator,
      negate = negate,
      startWindow = startWindow,
      endWindow = endWindow
    )
  
  return(subsetOperator)
}




#   # Helper function to create subset cohort window
#   createWindow <- function(startDay, endDay) {
#     tryCatch(
#       {
#         CohortGenerator::createSubsetCohortWindow(
#           startDay = startDay,
#           endDay = endDay,
#           targetAnchor = targetAnchor
#         )
#       },
#       error = function(e) {
#         stop("Error in creating subset cohort window: ", e$message)
#       }
#     )
#   }
#
#   # Define cohort subset windows
#   anytimeBeforeToAnytimeAfter <- createWindow(startDay, endDay)
#   anytimeBeforeTo1DayBefore <- createWindow(startDay, -1)
#   anyTimeBeforeToDay0 <- createWindow(startDay, 0)
#   onDay0 <- createWindow(0, 0)
#   onDay0ToAnytimeAfter <- createWindow(0, endDay)
#   day1ToAnytimeAfter <- createWindow(1, endDay)
#
#   windowVars <- list(
#     Before = anytimeBeforeTo1DayBefore,
#     OnOrBefore = anyTimeBeforeToDay0,
#     On = onDay0,
#     OnOrAfter = onDay0ToAnytimeAfter,
#     After = day1ToAnytimeAfter,
#     AnyTime = anytimeBeforeToAnytimeAfter
#   )
#
#   # Initialize a list to store results
#   results <- list()
#
#   # Create and assign cohort subsets to the results list
#   for (period in c("Before", "OnOrBefore", "On", "OnOrAfter", "After", "AnyTime")) {
#     for (negate in c(FALSE, TRUE)) {
#       suffix <- ifelse(negate, "_Negate", "")
#       windowVar <- windowVars[[period]]
#       objectName <-
#         paste0(
#           "limitWhenSubsetCohort_",
#           targetAnchorName,
#           "_",
#           period,
#           suffix
#         )
#       noLabel <- ifelse(period == "AnyTime", yes = TRUE, no = FALSE)
#       tryCatch(
#         {
#           results[[objectName]] <- CohortGenerator::createCohortSubset(
#             name = paste0(
#               ifelse(negate, " - without ", " - with "),
#               baseName,
#               ifelse(noLabel, "", no = paste0(" - ", tolower(period)))
#             ),
#             cohortIds = subsetCohortIds,
#             cohortCombinationOperator = cohortCombinationOperator,
#             negate = negate,
#             startWindow = windowVar,
#             endWindow = anytimeBeforeToAnytimeAfter
#           )
#         },
#         error = function(e) {
#           stop("Error in creating cohort subset: ", e$message)
#         }
#       )
#     }
#   }
#
#   return(results)
# }
