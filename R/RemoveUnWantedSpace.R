#' @export
removeUnWantedSpace <- function(text) {
  text <- trimws(text)

  # Remove multiple spaces in between words
  text <- gsub("\\s+", " ", text)
  return(text)
}
