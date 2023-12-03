#' @export
createGoodCohortNames <- function(text) {
  removeUnWantedSpace(
    removeStringWithinParenthesis(
      removeStringWithinSquareBracket(text = text)
    )
  )
}
