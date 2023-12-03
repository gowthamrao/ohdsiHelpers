#' @export
removeStringWithinSquareBracket <- function(text) {
  gsub(
    pattern = "_",
    replacement = " ",
    x = gsub(
      pattern = "\\[(.*?)\\]_",
      replacement = "",
      x = gsub(
        pattern = " ",
        replacement = "_",
        x = text
      )
    )
  )
}
