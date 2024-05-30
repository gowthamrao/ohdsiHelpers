#' @export
quoteAndJoinArray <- function(stringArray) {
  return(paste0(paste0("'", stringArray, "'"), collapse = ", "))
}