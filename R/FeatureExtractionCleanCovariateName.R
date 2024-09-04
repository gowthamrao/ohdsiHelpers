#' @export
featureExtractionCleanCovariateName <- function(inputString) {
  stringr::str_squish(stringr::str_trim(sub(".*?:\\s+", "", inputString)))
}