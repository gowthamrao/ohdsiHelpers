#' @export
getCohortIntersectViewResults <- function(inclusionStatsTable,
                                          rulesOfInterest,
                                          modeId = 0) {
  if ('id' %in% colnames(rulesOfInterest)) {
    rulesOfInterest <- rulesOfInterest |>
      dplyr::mutate(ruleSequence = id)
  }
  if ('name' %in% colnames(rulesOfInterest)) {
    rulesOfInterest <- rulesOfInterest |>
      dplyr::mutate(ruleName = name)
  }
  
  output <- inclusionStatsTable |>
    dplyr::filter(modeId == !!modeId,
                  ruleSequence %in% c(rulesOfInterest$ruleSequence - 1)) |>
    dplyr::arrange(databaseId, ruleSequence) |>
    dplyr::mutate(proportion = personCount / personTotal) |>
    dplyr::inner_join(rulesOfInterest |>
                        dplyr::select(ruleSequence,
                                      ruleName) |>
                        dplyr::distinct(),
                      by = "ruleSequence") |>
    dplyr::rename(cohortId = cohortDefinitionId,
                  id = ruleSequence,
                  name = ruleName) |>
    dplyr::mutate(
      report = OhdsiHelpers::formatCountPercent(count = personCount, percent = proportion),
      ruleName = paste0(id, " ", name)
    ) |>
    dplyr::arrange(cohortId, id) |>
    dplyr::mutate(id = id + 1)
  
  if ('databaseId' %in% colnames(output)) {
    output <- output |>
      dplyr::select(cohortId,
                    ruleName,
                    databaseId,
                    report) |>
      tidyr::pivot_wider(id_cols = ruleName,
                         names_from = databaseId,
                         values_from = report)
  } else {
    output <- output |>
      dplyr::select(cohortId,
                    ruleName,
                    report)
    
  }
  return(output)
}
