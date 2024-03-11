#' @export
getCohortCountAttritionWebApiGetCohortResultsOutput <-
  function(getCohortResultsOutput,
           cdmSources,
           modeId = 1) {
    sequenceSourceKey <- getCohortResultsOutput$summary |>
      dplyr::filter(.data$mode == modeId) |>
      dplyr::select(.data$sourceKey,
                    .data$finalCount) |>
      dplyr::arrange(dplyr::desc(.data$finalCount)) |>
      dplyr::select(.data$sourceKey) |>
      dplyr::mutate(sequenceSourceKey = dplyr::row_number())
    
    summary <-
      getCohortResultsOutput$summary |>
      dplyr::filter(.data$mode == modeId) |>
      dplyr::select(.data$sourceKey,
                    .data$baseCount,
                    .data$percentMatched) |>
      dplyr::mutate(
        id = 0,
        name = "base",
        countSatisfying = baseCount,
        percentExcluded = percentMatched
      ) |>
      dplyr::mutate(report = OhdsiHelpers::formatCountPercent(percent = percentExcluded, count = countSatisfying)) |>
      dplyr::inner_join(cdmSources |>
                          dplyr::select(.data$sourceKey,
                                        .data$database)) |>
      dplyr::select(.data$database,
                    .data$sourceKey,
                    .data$id,
                    .data$name,
                    .data$report)
    
    inclusionRule <-
      getCohortResultsOutput$inclusionRuleStats |>
      dplyr::mutate(id = .data$id + 1) |>
      dplyr::filter(.data$mode == modeId) |>
      dplyr::mutate(
        report = OhdsiHelpers::formatCountPercent(
          percent = .data$percentExcluded,
          count = .data$countSatisfying
        )
      ) |>
      dplyr::inner_join(cdmSources |>
                          dplyr::select(.data$sourceKey,
                                        .data$database)) |>
      dplyr::select(.data$database,
                    .data$sourceKey,
                    .data$id,
                    .data$name,
                    .data$report)
    
    finalCount <-
      getCohortResultsOutput$summary |>
      dplyr::filter(.data$mode == modeId) |>
      dplyr::select(.data$sourceKey,
                    .data$finalCount) |>
      dplyr::mutate(id = -1,
                    name = "final",
                    countSatisfying = finalCount) |>
      dplyr::mutate(report = OhdsiHelpers::formatIntegerWithComma(number = countSatisfying)) |>
      dplyr::inner_join(cdmSources |>
                          dplyr::select(.data$sourceKey,
                                        .data$database)) |>
      dplyr::select(.data$database,
                    .data$sourceKey,
                    .data$id,
                    .data$name,
                    .data$report)
    
    bothCombined <- dplyr::bind_rows(finalCount,
                                     summary,
                                     inclusionRule) |>
      dplyr::inner_join(sequenceSourceKey,
                        by = "sourceKey") |>
      dplyr::arrange(.data$sequenceSourceKey) |>
      tidyr::pivot_wider(
        id_cols = c(.data$id, .data$name),
        names_from = database,
        values_from = report
      ) |>
      dplyr::arrange(.data$id)
    
    return(bothCombined)
  }
