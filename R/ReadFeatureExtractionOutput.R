#' @export
readFeatureExtractionOutput <- function(rootFolder,
                                        filters = NULL) {
  output <- readOutput(
    rootFolder = rootFolder,
    filters = !!filters,
    name = "FeatureExtraction"
  )

  return(output)
}
