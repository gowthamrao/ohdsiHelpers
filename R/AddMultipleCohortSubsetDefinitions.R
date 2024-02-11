#' Add multiple cohort subset definitions
#'
#' This function adds multiple cohort subset definitions to a given cohort definition set.
#' It iterates over provided sequences of subset operators in a nested manner,
#' creating a unique subset definition for every possible combination of operators.
#'
#' @param cohortDefinitionSet A cohort definition set to which the subset definitions will be added.
#' @param targetCohortIds A vector of target cohort IDs.
#' @param namePrefix A prefix for the name of each cohort subset definition.
#' @param ... One or more vectors of subset operators.
#'
#' @return The modified cohort definition set with added subset definitions.
#'
#' @examples
#' cohortDefinitionSetWithSubset <- addMultipleCohortSubsetDefinitions(
#'   cohortDefinitionSet,
#'   targetOrIndicationCohortIds,
#'   subsetOperatorSequence1,
#'   subsetOperatorSequence2
#'   # ... Add more sequences if necessary
#' )
#' @export
addMultipleCohortSubsetDefinitions <-
  function(cohortDefinitionSet,
           targetCohortIds,
           namePrefix,
           ...) {
    subsetOperatorLists <- list(...)
    definitionId <- 1
    
    # Internal recursive function to process subsets
    processSubsets <- function(operators, currentOperators) {
      if (length(operators) == 0) {
        # Base case: no more operators to process, create and add subset definition
        subsetDefinition <-
          CohortGenerator::createCohortSubsetDefinition(
            name = namePrefix,
            definitionId = definitionId,
            subsetOperators = currentOperators
          )
        
        cohortDefinitionSet <<-
          CohortGenerator::addCohortSubsetDefinition(
            cohortDefinitionSet,
            cohortSubsetDefintion = subsetDefinition,
            targetCohortIds = targetCohortIds
          )
        
        definitionId <<- definitionId + 1
      } else {
        # Recursive case: iterate through the current operator list and process further
        for (operator in operators[[1]]) {
          processSubsets(operators[-1], c(currentOperators, list(operator)))
        }
      }
    }
    
    # Start the recursive process with the initial list of operator sequences
    processSubsets(subsetOperatorLists, list())
    return(cohortDefinitionSet)
  }
