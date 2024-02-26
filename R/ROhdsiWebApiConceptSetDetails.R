#' @export
rOhdsiWebApiConceptSetDetails <- function(baseUrl,
                                          conceptSetId) {
  conceptSetExpression <-
    ROhdsiWebApi::getConceptSetDefinition(
      conceptSetId = conceptSetId,
      baseUrl = baseUrl
    )

  conceptSetExpressionTable <-
    ROhdsiWebApi::convertConceptSetDefinitionToTable(conceptSetExpression) |>
    dplyr::select(
      "conceptId",
      "conceptName",
      "vocabularyId",
      "conceptClassId",
      "standardConcept",
      "includeDescendants"
    )

  # resolve concepts
  resolveConceptSets <-
    ROhdsiWebApi::resolveConceptSet(
      conceptSetDefinition = conceptSetExpression,
      baseUrl = baseUrl
    )
  # mapped concept set expression
  sourceConcepts <-
    ROhdsiWebApi::getSourceConcepts(
      conceptIds = resolveConceptSets,
      baseUrl = baseUrl
    )

  # conceptIdDetails
  conceptIdDetails <-
    ROhdsiWebApi::getConcepts(
      conceptIds = c(
        conceptSetExpressionTable$conceptId,
        resolveConceptSets,
        sourceConcepts$conceptId
      ) |>
        unique(),
      baseUrl = baseUrl
    )

  output <- c()
  output$concepts <-
    conceptIdDetails |>
    dplyr::distinct() |>
    dplyr::arrange(conceptId)
  output$resolved <- resolveConceptSets |>
    unique() |>
    sort()
  output$mapped <- sourceConcepts$conceptId |>
    unique() |>
    sort()
  output$expression <- conceptSetExpression
  output$expressionTable <- conceptSetExpressionTable

  return(output)
}
