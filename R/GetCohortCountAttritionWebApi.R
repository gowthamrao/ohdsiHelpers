#' @export
getCohortCountAttritionWebApiGetCohortResultsOutput <-
  function(getCohortResultsOutput,
           cdmSources,
           modeId = 1) {
    sequenceSourceKey <- cohortCountsFromWebApi$summary |>
      dplyr::filter(mode == modeId) |>
      dplyr::select(sourceKey,
                    finalCount) |>
      dplyr::arrange(dplyr::desc(finalCount)) |>
      dplyr::select(sourceKey) |>
      dplyr::mutate(sequenceSourceKey = dplyr::row_number())
    
    
    summary <-
      cohortCountsFromWebApi$summary |>
      dplyr::filter(mode == modeId) |>
      dplyr::select(sourceKey,
                    baseCount,
                    percentMatched) |>
      dplyr::mutate(
        id = 0,
        name = "base",
        countSatisfying = baseCount,
        percentExcluded = percentMatched
      ) |>
      dplyr::mutate(report = OhdsiHelpers::formatCountPercent(percent = percentExcluded, count = countSatisfying)) |>
      dplyr::inner_join(cdmSources |>
                          dplyr::select(sourceKey,
                                        database)) |>
      dplyr::select(database,
                    sourceKey,
                    id,
                    name,
                    report)
    
    inclusionRule <-
      cohortCountsFromWebApi$inclusionRuleStats |>
      dplyr::mutate(id = id + 1) |>
      dplyr::filter(mode == modeId) |>
      dplyr::mutate(report = OhdsiHelpers::formatCountPercent(percent = percentExcluded, count = countSatisfying)) |>
      dplyr::inner_join(cdmSources |>
                          dplyr::select(sourceKey,
                                        database)) |>
      dplyr::select(database,
                    sourceKey,
                    id,
                    name,
                    report)
    
    bothCombined <- dplyr::bind_rows(summary,
                                     inclusionRule) |>
      dplyr::inner_join(sequenceSourceKey,
                        by = "sourceKey") |>
      dplyr::arrange(sequenceSourceKey) |>
      tidyr::pivot_wider(
        id_cols = c(id, name),
        names_from = database,
        values_from = report
      ) |>
      dplyr::arrange(id)
    
    return(bothCombined)
  }