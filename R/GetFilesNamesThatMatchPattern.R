#' Retrieve file names that match any of the given patterns.
#'
#' This function searches for files within the current working directory (and its subdirectories)
#' that match any of the patterns provided. Due to performance considerations with large sets of patterns,
#' the patterns are processed in chunks.
#'
#' @param patterns A character vector containing file name patterns to match against.
#' @param chunkSize The number of patterns to process in each regex compilation, defaults to 1000.
#' @return A character vector containing the full paths of files that match any of the provided patterns.
#' @export
getFilesNamesThatMatchPattern <-
  function(patterns, chunkSize = 1000) {
    # Prepare for storing results
    matchedFiles <- character()
    
    # Loop over chunks
    for (i in seq(1, length(patterns), by = chunkSize)) {
      # Create the current chunk of patterns
      chunk <- patterns[i:min(i + chunkSize - 1, length(patterns))]
      # Generate the regex pattern for the current chunk
      pattern <- paste0(chunk, collapse = "|")
      
      # Get files matching the current pattern
      files <- list.files(
        path = getwd(),
        pattern = pattern,
        all.files = TRUE,
        full.names = TRUE,
        recursive = TRUE,
        ignore.case = TRUE,
        include.dirs = TRUE
      )
      
      # Add matched files to the list
      matchedFiles <- c(matchedFiles, files)
    }
    
    # Return unique matched files
    unique(matchedFiles)
  }