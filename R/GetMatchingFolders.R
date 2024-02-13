#' @export
getMatchingFolders <- function(rootPath, matchString) {
  # List all files and folders recursively from the root directory
  allPaths <- list.files(rootPath, full.names = TRUE, recursive = TRUE, include.dirs = TRUE)

  # Filter only folders
  folderPaths <- allPaths[file.info(allPaths)$isdir]

  # Filter folders that contain the matchString
  matchingFolders <- grep(pattern = matchString, x = folderPaths, value = TRUE) |>
    unique()

  if ((matchingFolders |> length()) > 1) {
    stop(paste0(
      "More than one folder with the same name in path.",
      paste0(matchingFolders, collapse = "; ")
    ))
  }

  return(matchingFolders)
}
