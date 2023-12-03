#' @export
generateRandomString <- function() {
  randomStringTableName <-
    tolower(paste0(
      "tmp_",
      paste0(
        sample(
          x = c(LETTERS, 0:9),
          size = 12,
          replace = TRUE
        ),
        collapse = ""
      )
    ))
  return(randomStringTableName)
}
