#' @export
createTable1SpecificationsForCohortFeatures <- function(analysisId,
                                                        cohortIds,
                                                        label = "Feature cohorts") {
  if (!length(analysisId) == 1) {
    stop("only one analysis id")
  }
  
  if (!length(label) == 1) {
    stop("only one label")
  }
  
  output <- dplyr::tibble(label = label,
                          analysisId = analysisId,
                          cohortIds = paste0(((cohortIds * 1000) + analysisId), ","))
  
  return(output)
}