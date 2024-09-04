#' @export
featureExtractionTimeRefToName <- function(startDay, endDay, noSpace = TRUE) {
  startPrefix <- ifelse(startDay < 0, "p", "")
  endPrefix <- ifelse(endDay < 0, "p", "")
  text <- paste0(startPrefix,
                 as.integer(abs(startDay)),
                 " to ",
                 endPrefix,
                 as.integer(abs(endDay)))
  
  if (noSpace) {
    text <- gsub(" ", "", text)
  }
  
  return(text)
}