#' @export
createCohortExplorerAppFromOutputs <- function(cohortExplorerFiles,
                                               outputLocation) {
  dir.create(
    path = file.path(outputLocation, "data"),
    showWarnings = FALSE,
    recursive = TRUE
  )
  
  CohortExplorer::exportCohortExplorerAppFiles(outputLocation)
  
  content <- c(
    "Version: 1.0",
    "RestoreWorkspace: Default",
    "SaveWorkspace: Default",
    "AlwaysSaveHistory: Default",
    "",
    "EnableCodeIndexing: Yes",
    "UseSpacesForTab: Yes",
    "NumSpacesForTab: 2",
    "Encoding: UTF-8",
    "",
    "RnwWeave: Sweave",
    "LaTeX: pdfLaTeX"
  )
  
  writeLines(content, con = file.path(outputLocation,
                                      "CohortExplorer.Rproj"))
  
  for (i in (1:length(cohortExplorerFiles))) {
    file.copy(from = cohortExplorerFiles[[i]], to = file.path(outputLocation, "data"))
  }
  
  
}