# Function to update the sql in a CohortDefinitionSet
#' @export
updateSqlInCohortDefinitionSet <- function(cohortDefinitionSet) {
  circeOptions <- CirceR::createGenerateOptions(generateStats = TRUE)
  
  for (i in (1:nrow(cohortDefinitionSet))) {
    json <- cohortDefinitionSet[i, ]$json
    sql <-
      CirceR::buildCohortQuery(expression = json, options = circeOptions)
    cohortDefinitionSet[i, ]$sql <- sql
  }
  return(cohortDefinitionSet)
}
