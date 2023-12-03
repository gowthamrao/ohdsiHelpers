#' @export
createGoodCohortNames <- function(text) {
  OhdsiHelpers::removeUnWantedSpace(
    OhdsiHelpers::removeStringWithinParenthesis(
      OhdsiHelpers::removeStringWithinSquareBracket(text = text)
    )
  )
}
