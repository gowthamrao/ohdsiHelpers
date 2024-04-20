#' @export
createPheValuatorAnalysisList <- function(cutPoints = c("EV"),
                                          phenotypeCohortIds,
                                          washoutPeriod = 365,
                                          cohortDefinitionSet,
                                          cohortArgsAcute,
                                          startingAnalysisId = 0) {
  pheValuatorAnalysisList <- list()
  
  for (i in (1:length(phenotypeCohortIds))) {
    phenotypeCohortId <- phenotypeCohortIds[[i]]
    cohortName <- cohortDefinitionSet |>
      dplyr::filter(cohortId == phenotypeCohortId) |>
      dplyr::pull(cohortName)
    
    conditionAlgTestArgs <-
      PheValuator::createTestPhenotypeAlgorithmArgs(
        cutPoints =  cutPoints,
        phenotypeCohortId = phenotypeCohortId,
        washoutPeriod = washoutPeriod
      )
    
    analysis <- PheValuator::createPheValuatorAnalysis(
      analysisId = startingAnalysisId + i,
      description = cohortName,
      createEvaluationCohortArgs = cohortArgsAcute,
      testPhenotypeAlgorithmArgs = conditionAlgTestArgs
    )
    pheValuatorAnalysisList[[i]] <- analysis
  }
  
  return(pheValuatorAnalysisList)
  
}
