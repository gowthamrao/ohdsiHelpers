#' @export
getCohortCountsAndInclusionRulesInParallel <-
  function(cdmSources = NULL,
           cohortTableNames,
           sequence = 1,
           databaseIds = NULL,
           cohortIds = NULL,
           cohortDefinitionSet = NULL) {
    if (is.null(cohortTableNames$cohortSummaryStats)) {
      stop("'cohortSummaryStats' not found in cohortTableNames")
    }
    if (is.null(cohortTableNames$cohortInclusionResultTable)) {
      stop("'cohortInclusionResultTable' not found in cohortTableNames")
    }
    if (is.null(cohortTableNames$cohortInclusionStatsTable)) {
      stop("'cohortInclusionStatsTable' not found in cohortTableNames")
    }
    if (is.null(cohortTableNames$cohortSummaryStatsTable)) {
      stop("'cohortSummaryStatsTable' not found in cohortTableNames")
    }
    if (is.null(cohortTableNames$cohortCensorStatsTable)) {
      stop("'cohortCensorStatsTable' not found in cohortTableNames")
    }
    
    output <- c()
    
    if (!is.null(cohortDefinitionSet)) {
      if (!is.null(cohortIds)) {
        cohortDefinitionSet <- cohortDefinitionSet |>
          dplyr::filter(cohortId %in% cohortIds)
      }
      inclusionRulesCaptured <- c()
      for (i in (1:nrow(cohortDefinitionSet))) {
        json <- cohortDefinitionSet[i,]$json
        cohortExpression <-
          RJSONIO::fromJSON(content = json, digits = 23)
        inclusionRules <- cohortExpression$InclusionRules
        if (!is.null(inclusionRules)) {
          captureInclusionRules <- c()
          for (j in (1:length(inclusionRules))) {
            captureInclusionRules[[j]] <- dplyr::tibble(
              id = j,
              name = as.character(inclusionRules[[j]]$name),
              description = if (is.null(inclusionRules[[j]]$description))
                ""
              else
                inclusionRules[[j]]$description
            )
          }
          captureInclusionRules <-
            dplyr::bind_rows(captureInclusionRules) |>
            dplyr::mutate(cohortId = cohortDefinitionSet[i,]$cohortId) |>
            dplyr::select(cohortId,
                            id,
                            name,
                            description)
        }
        inclusionRulesCaptured[[i]] <- captureInclusionRules
      }
      inclusionRulesCaptured <-
        dplyr::bind_rows(inclusionRulesCaptured) |>
        dplyr::arrange(cohortId,
                       id)
      output$cohortInclusion <- inclusionRulesCaptured
    }
    
    writeLines("getting cohort counts")
    output$cohortCounts <-
      getCohortCountsInParallel(
        cdmSources = cdmSources,
        cohortTableName = cohortTableNames$cohortTable,
        sequence = sequence,
        databaseIds = databaseIds,
        cohortIds = cohortIds
      )$cohortCounts
    
    
    writeLines("getting cohort summary stats")
    output$cohortSummaryStats <-
      getCohortSummaryStatsInParallel(
        cdmSources = cdmSources,
        cohortTableNames = cohortTableNames,
        sequence = sequence,
        databaseIds = databaseIds,
        cohortIds = cohortIds
      )
    
    writeLines("getting cohort inclusion results")
    output$cohortInclusionResultTable <-
      getCohortInclusionResultsInParallel(
        cdmSources = cdmSources,
        cohortTableNames = cohortTableNames,
        sequence = sequence,
        databaseIds = databaseIds,
        cohortIds = cohortIds
      )
    
    writeLines("getting cohort inclusion stats")
    output$cohortInclusionStatsTable <-
      getCohortIncStatsInParallel(
        cdmSources = cdmSources,
        cohortTableNames = cohortTableNames,
        sequence = sequence,
        databaseIds = databaseIds,
        cohortIds = cohortIds
      )
    
    writeLines("getting cohort censor stats")
    output$cohortCensorStatsTable <-
      getCohortCensorStatsInParallel(
        cdmSources = cdmSources,
        cohortTableNames = cohortTableNames,
        sequence = sequence,
        databaseIds = databaseIds,
        cohortIds = cohortIds
      )
    
    return(output)
  }
