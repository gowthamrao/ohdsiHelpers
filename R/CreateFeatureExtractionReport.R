#' @export
createFeatureExtractionReport <- function(characterization,
                                          cohortIds = NULL,
                                          meanThreshold = 0.01,
                                          analysisNames = NULL,
                                          domainIds = NULL,
                                          startDays = NULL,
                                          endDays = NULL,
                                          splitCovariateName = TRUE,
                                          cohortDefinitionSet = NULL) {
  if (!is.null(cohortIds)) {
    characterization$covariates <- characterization$covariates |>
      dplyr::filter(cohortId %in% c(cohortIds))
  }
  
  if (!is.null(meanThreshold)) {
    characterization$covariates <- characterization$covariates |>
      dplyr::filter(mean >= meanThreshold)
  }
  
  if (!is.null(analysisNames)) {
    characterization$analysisRef <- characterization$analysisRef |>
      dplyr::filter(characterization$analysisRef$analysisName %in% c(analysisNames))
    
    characterization$covariates <- characterization$covariates |>
      dplyr::filter(analysisId %in% c(characterization$analysisRef$analysisId |> unique()))
  }
  
  if (!is.null(domainIds)) {
    characterization$analysisRef <- characterization$analysisRef |>
      dplyr::filter(characterization$analysisRef$domainId %in% c(domainIds))
    
    characterization$covariates <- characterization$covariates |>
      dplyr::filter(analysisId %in% c(characterization$analysisRef$analysisId |> unique()))
  }
  
  if (!is.null(startDays)) {
    characterization$timeRef <- characterization$timeRef |>
      dplyr::filter(characterization$timeRef$startDay %in% c(startDays))
    
    characterization$covariates <- characterization$covariates |>
      dplyr::filter(timeId %in% c(characterization$timeRef$timeId |> unique()))
  }
  
  if (!is.null(endDays)) {
    characterization$timeRef <- characterization$timeRef |>
      dplyr::filter(characterization$timeRef$endDay %in% c(endDays))
    
    characterization$covariates <- characterization$covariates |>
      dplyr::filter(timeId %in% c(characterization$timeRef$timeId |> unique()))
  }
  
  output <- characterization$covariates |>
    dplyr::inner_join(characterization$timeRef)
  characterization$covariates <- NULL
  
  output <- output |>
    dplyr::inner_join(characterization$covariateRef)
  characterization$covariateRef <- NULL
  
  output <- output |>
    dplyr::inner_join(characterization$analysisRef)
  characterization$analysisRef <- NULL
  
  output <- output |>
    dplyr::mutate(covariateConceptId = (covariateId - analysisId) / 1000) 
  
  fields <- intersect(
    colnames(output),
    c(
      "cohortId",
      "databaseId",
      "startDay",
      "endDay",
      "covariateConceptId",
      "conceptId",
      "analysisId",
      "covariateId",
      "cohortId",
      "covariateName",
      "analysisName",
      "domainId",
      "mean",
      "sumValue"
    )
  )
  
  output <- output |>
    dplyr::select(dplyr::all_of(fields)) |>
    dplyr::mutate(mean = round(x = mean, digits = 2))
  
  if (!is.null(cohortDefinitionSet)) {
    output <- output |>
      dplyr::left_join(cohortDefinitionSet |>
                         dplyr::select(cohortId,
                                       cohortName),
                       by = "cohortId")
  }
  
  output <- output |>
    dplyr::arrange(cohortId,
                   startDay,
                   endDay,
                   dplyr::desc(mean))
  
  if (splitCovariateName) {
    output <- output |>
      splitDataFrameField(
        fieldName = "covariateName",
        splitChar = ": ",
        newFieldNames = c("featureExtractionPrefix",
                          "covariateName")
      ) |>
      dplyr::mutate(
        featureExtractionPrefix = stringr::str_squish(featureExtractionPrefix),
        covariateName = stringr::str_squish(covariateName)
      )
  }
  
  return(output)
}
