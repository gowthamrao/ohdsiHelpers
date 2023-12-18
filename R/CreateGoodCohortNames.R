#' @export
createGoodCohortNames <- function(text) {
  removeUnWantedSpace(removeStringWithinParenthesis(removeStringWithinSquareBracket(removeTrailingCommas(text = text))))
}
