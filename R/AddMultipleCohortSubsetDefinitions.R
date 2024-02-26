#' Add Multiple Cohort Subset Definitions with Progress Bar
#'
#' This function iteratively adds cohort subset definitions to a given cohort definition set.
#' It processes multiple sequences of subset operators, first adding each operator from the first sequence individually,
#' and then combining each operator from the first sequence with each operator from the subsequent sequences in turn.
#' A progress bar and console messages indicate the progress of the operation.
#'
#' @param cohortDefinitionSet The initial cohort definition set to which subset definitions will be added.
#' @param targetCohortIds A vector of target cohort IDs that the subset definitions will be applied to.
#' @param prefix A string prefix for the names of the subset definitions.
#' @param ... Additional arguments representing sequences of subset operators.
#' @return The modified cohort definition set with added subset definitions.
#' @export
#'
#' @examples
#' # Usage:
#' # cohortDefinitionSetWithSubset <- addMultipleCohortSubsetDefinitions(
#' #   cohortDefinitionSet,
#' #   targetOrIndicationCohortIds,
#' #   "CustomPrefix",
#' #   subsetOperatorSequence1,
#' #   subsetOperatorSequence2
#' #   # ... Add more sequences if necessary
#' # )
addMultipleCohortSubsetDefinitions <-
  function(cohortDefinitionSet,
           targetCohortIds,
           prefix,
           ...) {
    # Extract the variable list of operator sequences.
    subsetOperatorSequences <- list(...)
    totalDefinitions <-
      Reduce("*", lapply(subsetOperatorSequences, length))
    definitionId <- 1

    writeLines(paste0(" - adding ", totalDefinitions, " definitions."))

    # Initialize the progress bar
    pb <- txtProgressBar(min = 0, max = totalDefinitions, style = 3)

    # Internal recursive function to add subset definitions.
    # currentOperators: A list of current operators to be included in the subset definition.
    # remainingSequences: A list of remaining operator sequences to be processed.

    patternToReplaceDefaultValue <- "#####"
    addSubsetDefinitions <-
      function(currentOperators, remainingSequences, patternToReplaceDefaultValue) {
        # Create and add a subset definition if currentOperators is not empty.
        if (length(currentOperators) > 0) {
          subsetDefinition <-
            CohortGenerator::createCohortSubsetDefinition(
              name = prefix,
              definitionId = definitionId,
              subsetOperators = currentOperators,
              operatorNameConcatString = patternToReplaceDefaultValue # replacing default value of "," because it add unexpected ","
            )

          cohortDefinitionSet <<-
            CohortGenerator::addCohortSubsetDefinition(
              cohortDefinitionSet,
              cohortSubsetDefintion = subsetDefinition,
              targetCohortIds = targetCohortIds
            )

          # Update progress
          setTxtProgressBar(pb, definitionId)
          definitionId <<- definitionId + 1
        }

        # Process the next level of operators if remaining sequences are available.
        if (length(remainingSequences) > 0) {
          for (operator in remainingSequences[[1]]) {
            addSubsetDefinitions(
              currentOperators = c(currentOperators, operator),
              remainingSequences = remainingSequences[-1],
              patternToReplaceDefaultValue = patternToReplaceDefaultValue
            )
          }
        }
      }

    # Iterate over each sequence and process them.
    for (i in seq_along(subsetOperatorSequences)) {
      addSubsetDefinitions(
        currentOperators = list(),
        remainingSequences = subsetOperatorSequences[i:length(subsetOperatorSequences)],
        patternToReplaceDefaultValue = patternToReplaceDefaultValue
      )
    }

    # Close the progress bar
    close(pb)

    cohortDefinitionSet <- cohortDefinitionSet |>
      dplyr::mutate(
        cohortName = stringr::str_replace_all(
          string = cohortName,
          pattern = patternToReplaceDefaultValue,
          replacement = ""
        )
      )

    return(cohortDefinitionSet)
  }
