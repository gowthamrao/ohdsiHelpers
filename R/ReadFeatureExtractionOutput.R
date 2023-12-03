#' @export
readFeatureExtractionOutput <- function(rootFolder,
                                        filters = NULL) {
  output <- readOutput(rootFolder = rootFolder,
                       filter = filters,
                       name = "FeatureExtraction")
  
  return(output)
}
