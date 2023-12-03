#' @export
removeStringWithinParenthesis <- function(text) {
  gsub(
    pattern = "_",
    replacement = " ",
    x = gsub(
      pattern = "\\(.*?\\)",
      replacement = "",
      x = gsub(
        pattern = " ",
        replacement = "_",
        x = text
      )
    )
  )
}
