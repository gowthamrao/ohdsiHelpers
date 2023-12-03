#' @export
normalizeFilePath <- function(filePath) {
  # Normalize the file path based on the operating system
  normalizedPath <- normalizePath(path = filePath, winslash = "/", mustWork = FALSE)
  
  return(normalizedPath)
}