#' @export
deleteCohortExplorerOutputs <- function(outputFolder) {
  allDirs <-
    list.dirs(path = outputFolder,
              full.names = TRUE,
              recursive = TRUE)
  allDirsFiltered <- allDirs[grepl('CohortExplorer', allDirs)]
  allDirsFiltered <-
    allDirsFiltered[endsWith(x = allDirsFiltered, suffix = "CohortExplorer")]
  
  for (i in (1:length(allDirsFiltered))) {
    unlink(x = allDirsFiltered[[i]],
           recursive = TRUE,
           force = TRUE)
  }
}