#' Returns list with circe generated cohort documentation
#'
#' @description
#' Returns list with circe generated cohort documentation
#'
#' @param cohortDefinition An R object (list) with a list representation of the cohort definition expression,
#'                          that may be converted to a cohort expression JSON using
#'                          RJSONIO::toJSON(x = cohortDefinition, digits = 23, pretty = TRUE)
#'
#' @param cohortName Name for the cohort definition
#'
#' @param includeConceptSets Do you want to included concept set in the documentation
#'
#' @param addUsefulHeaders Do you want useful headers like cohort name?
#'
#' @return list object
#'
#' @export
getCirceHumanReadableCohortText <- function(cohortDefinition,
                                            cohortName = "Cohort Definition",
                                            includeConceptSets = FALSE,
                                            addUsefulHeaders = TRUE) {
  cohortJson <-
    RJSONIO::toJSON(
      x = cohortDefinition,
      digits = 23,
      pretty = TRUE
    )
  circeExpression <-
    CirceR::cohortExpressionFromJson(expressionJson = cohortJson)
  circeExpressionMarkdown <-
    CirceR::cohortPrintFriendly(circeExpression)
  circeConceptSetListmarkdown <-
    CirceR::conceptSetListPrintFriendly(circeExpression$conceptSets)

  if (addUsefulHeaders) {
    circeExpressionMarkdown <-
      paste0(
        "# ",
        cohortName,
        "\r\n\r\n",
        circeExpressionMarkdown
      )
  }

  if (includeConceptSets) {
    circeExpressionMarkdown <-
      paste0(
        circeExpressionMarkdown,
        "\r\n\r\n",
        "\r\n\r\n",
        "## Concept Sets:",
        "\r\n\r\n",
        circeConceptSetListmarkdown
      )
  }

  htmlExpressionCohort <-
    markdown::renderMarkdown(text = circeExpressionMarkdown)
  htmlExpressionConceptSetExpression <-
    markdown::renderMarkdown(text = circeConceptSetListmarkdown)
  return(
    list(
      cohortJson = cohortJson,
      cohortMarkdown = circeExpressionMarkdown,
      conceptSetMarkdown = circeConceptSetListmarkdown,
      cohortHtmlExpression = htmlExpressionCohort,
      conceptSetHtmlExpression = htmlExpressionConceptSetExpression
    )
  )
}
