#' @export
getCohortAttritionViewResults <- function(inclusionResultTable,
                                          cohortSummaryStats = NULL,
                                          maxRuleId) {
  numberToBitString <- function(numbers) {
    sapply(numbers, function(number) {
      if (number == 0) {
        return("0")
      }
      
      bitString <- ""
      while (number > 0) {
        bitString <- paste0(as.character(number %% 2), bitString)
        number <- number %/% 2
      }
      
      bitString
    })
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
  
  for (i in (1:maxRuleId)) {
    if ('databaseId' %in% colnames(inclusionResultTable)) {
      output[[i]] <- inclusionResultTable |>
        dplyr::filter(endsWith(
          x = numberToBitString(inclusionRuleMask),
          suffix = numberToBitString(ruleToMask(i))
        )) |>
        dplyr::group_by(databaseId,
                        cohortDefinitionId,
                        modeId) |>
        dplyr::summarise(personCount = sum(personCount),
                         .groups = "drop") |>
        dplyr::ungroup() |>
        dplyr::mutate(id = i)
    } else {
      output[[i]] <- inclusionResultTable |>
        dplyr::filter(endsWith(
          x = numberToBitString(inclusionRuleMask),
          suffix = numberToBitString(ruleToMask(i))
        )) |>
        dplyr::group_by(cohortDefinitionId,
                        modeId) |>
        dplyr::summarise(personCount = sum(personCount),
                         .groups = "drop") |>
        dplyr::ungroup() |>
        dplyr::mutate(id = i)
    }
  }
  
  output <- dplyr::bind_rows(output)
  
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
      dplyr::select(-baseCount)
  }
  
  return(output)
}
