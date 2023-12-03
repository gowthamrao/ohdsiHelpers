#' @export
createGoodCohortNames <- function(text) {
  ohdsiHelpers::removeUnWantedSpace(
    ohdsiHelpers::removeStringWithinParenthesis(
      ohdsiHelpers::removeStringWithinSquareBracket(text = text)
    )
  )
}
