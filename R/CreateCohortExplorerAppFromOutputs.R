#' @export
createCohortExplorerAppFromOutputs <- function(cohortExplorerFiles,
                                               outputLocation) {
  dir.create(
    path = file.path(outputLocation, "data"),
    showWarnings = FALSE,
    recursive = TRUE
  )

  CohortExplorer::exportCohortExplorerAppFiles(outputLocation)

  for (i in (1:length(cohortExplorerFiles))) {
    file.copy(from = cohortExplorerFiles[[i]], to = file.path(outputLocation, "data"))
  }
}
