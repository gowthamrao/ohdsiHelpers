#' @export
getCohortAttritionViewResults <- function(inclusionResultTable,
                                          cohortSummaryStats = NULL,
                                          rulesOfInterest,
                                          modeId = NULL,
                                          cohortIds = NULL) {
  if (!is.null(cohortIds)) {
    inclusionResultTable <-
      inclusionResultTable |> dplyr::filter(cohortDefinitionId %in% c(cohortIds))
  }
  
  if ('id' %in% colnames(rulesOfInterest)) {
    rulesOfInterest <- rulesOfInterest |>
      dplyr::mutate(ruleSequence = id)
  }
  if ('name' %in% colnames(rulesOfInterest)) {
    rulesOfInterest <- rulesOfInterest |>
      dplyr::mutate(ruleName = name)
  }
  
  maxRuleId <- rulesOfInterest$ruleSequence |> max()
  
  numberToBitString <- function(numbers) {
    vapply(numbers, function(number) {
      if (number == 0) {
        return("0")
      }
      
      bitString <- character()
      while (number > 0) {
        bitString <- c(as.character(number %% 2), bitString)
        number <- number %/% 2
      }
      
      paste(bitString, collapse = "")
    }, character(1))
  }
  
  # problem - how to create attrition view
  bitsToMask <- function(bits) {
    positions <- seq_along(bits) - 1
    number <- sum(bits * 2 ^ positions)
    return(number)
  }
  
  ruleToMask <- function(ruleId) {
    bits <- rep(1, ruleId)
    mask <- bitsToMask(bits)
    return(mask)
  }
  
  output <- c()
  
  if (!is.null(modeId)) {
    inclusionResultTable <-
      inclusionResultTable |> dplyr::filter(modeId == !!modeId)
  }
  
  inclusionResultTable <- inclusionResultTable |>
    dplyr::mutate(inclusionRuleMaskBitString = numberToBitString(inclusionRuleMask))
  
  for (i in (1:maxRuleId)) {
    suffixString <- numberToBitString(ruleToMask(i))
    if ('databaseId' %in% colnames(inclusionResultTable)) {
      output[[i]] <- inclusionResultTable |>
        dplyr::filter(endsWith(x = inclusionRuleMaskBitString,
                               suffix = suffixString)) |>
        dplyr::group_by(databaseId,
                        cohortDefinitionId,
                        modeId) |>
        dplyr::summarise(personCount = sum(personCount),
                         .groups = "drop") |>
        dplyr::ungroup() |>
        dplyr::mutate(ruleSequence = i)
    } else {
      output[[i]] <- inclusionResultTable |>
        dplyr::filter(endsWith(x = inclusionRuleMaskBitString,
                               suffix = suffixString)) |>
        dplyr::group_by(cohortDefinitionId,
                        modeId) |>
        dplyr::summarise(personCount = sum(personCount),
                         .groups = "drop") |>
        dplyr::ungroup() |>
        dplyr::mutate(id = i)
    }
  }
  
  output <- dplyr::bind_rows(output) |>
    dplyr::inner_join(rulesOfInterest |>
                        dplyr::select(ruleSequence,
                                      ruleName) |>
                        dplyr::distinct(),
                      by = "ruleSequence")
  
  if (!is.null(cohortSummaryStats)) {
    output <- output |>
      dplyr::inner_join(
        cohortSummaryStats |>
          dplyr::select(cohortDefinitionId,
                        databaseId,
                        modeId,
                        baseCount),
        by = c("databaseId", "modeId", "cohortDefinitionId")
      ) |>
      dplyr::mutate(proportion = personCount / baseCount) |>
      dplyr::select(-baseCount) |>
      dplyr::mutate(report = OhdsiHelpers::formatCountPercent(count = personCount,
                                                              percent = proportion))
  }
  
  if ('databaseId' %in% colnames(inclusionResultTable)) {
    if ('report' %in% colnames(output)) {
      output <- output |>
        tidyr::pivot_wider(
          id_cols = c(cohortDefinitionId, modeId, ruleSequence, ruleName),
          names_from = databaseId,
          values_from = report
        )
    } else {
      output <- output |>
        tidyr::pivot_wider(
          id_cols = c(cohortDefinitionId, modeId, ruleSequence, ruleName),
          names_from = databaseId,
          values_from = personCount
        )
    }
  }
  
  output <- output |>
    dplyr::arrange(cohortDefinitionId, modeId, ruleSequence, ruleName) |> 
    dplyr::rename(cohortId = cohortDefinitionId)
  
  return(output)
}
