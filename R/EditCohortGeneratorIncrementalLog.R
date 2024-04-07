#' @export
editCohortGeneratorIncrementalLogInParallel <-
  function(incrementalFolder,
           incrementalFileName = "GeneratedCohorts.csv",
           deleteCohortIds = NULL,
           keepCohortIds = NULL) {
    incrementalFiles <- list.files(
      path = incrementalFolder,
      pattern = incrementalFileName,
      all.files = TRUE,
      full.names = TRUE,
      recursive = TRUE,
      ignore.case = TRUE
    )
    
    if (length(incrementalFiles) == 0) {
      stop("no incremental files found")
    }
    
    for (i in (1:length(incrementalFiles))) {
      OhdsiHelpers::editCohortGeneratorIncrementalLog(
        incrementalFolder = dirname(incrementalFiles[[i]]),
        incrementalFileName = basename(incrementalFiles[[i]]),
        keepCohortIds = cohortIdsToKeep
      )
    }
  }



#' Edit Incremental Cohort Generator Log
#'
#' This function edits a CSV file containing information about cohorts,
#' typically used in a cohort generation process. It allows for incremental
#' updates to the log file by either adding new data or deleting specified cohorts.
#'
#' @param incrementalFolder A string specifying the path to the folder where the
#'        incremental log file is located.
#' @param incrementalFileName A string specifying the name of the CSV file to be
#'        edited. Defaults to "GeneratedCohorts.csv".
#' @param deleteCohortIds A numeric vector of cohort IDs to be removed from the
#'        log file. If NULL, no deletions are made. Defaults to NULL.
#' @return The function does not return anything, but it updates the specified
#'         CSV file in the given folder.
#' @examples
#' editCohortGeneratorIncrementalLog("path/to/folder", deleteCohortIds = c(101, 102))
#'
#' @export
editCohortGeneratorIncrementalLog <- function(incrementalFolder,
                                              incrementalFileName = "GeneratedCohorts.csv",
                                              deleteCohortIds = NULL,
                                              keepCohortIds = NULL) {
  # Read the existing CSV file into a data frame
  cohortGenerator <-
    readr::read_csv(
      file = file.path(incrementalFolder, incrementalFileName),
      col_types = readr::cols()
    )
  
  convertToDateTime <- function(dataFrame, columnName) {
    dataFrame[[columnName]] <-
      as.POSIXct(dataFrame[[columnName]], format = "%Y/%m/%d %H:%M:%S")
    return(dataFrame)
  }
  
  cohortGenerator <- convertToDateTime(dataFrame = cohortGenerator,
                                       columnName = "timeStamp")
  
  # If there are cohort IDs specified for deletion, filter them out
  if (!is.null(deleteCohortIds)) {
    cohortGenerator <- cohortGenerator |>
      dplyr::filter(!.data$cohortId %in% deleteCohortIds)
  }
  
  # If there are cohort IDs specified for keep
  if (!is.null(keepCohortIds)) {
    cohortGenerator <- cohortGenerator |>
      dplyr::filter(.data$cohortId %in% keepCohortIds)
  }
  
  # Write the updated data frame back to the CSV file
  readr::write_csv(
    x = cohortGenerator,
    file = file.path(incrementalFolder, incrementalFileName),
    append = FALSE
  )
}
