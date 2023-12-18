#' @export
removeTrailingCommas <- function(text, recursive = TRUE) {
  # Using regex to remove trailing commas
  if (recursive) {
    # Remove all contiguous trailing commas
    return(sub("([, ]+, *)+$", "", text))
  } else {
    # Remove only the last trailing comma if it's contiguous
    return(sub(",[ ,]*$", "", text))
  }
}
