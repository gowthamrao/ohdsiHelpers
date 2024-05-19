
#' @export
getFeatureExtractionDefaultTemporalCovariateSettings <-
  function(timeWindows = OhdsiHelpers::getFeatureExtractionDefaultTimeWindows()) {
    
    feTemporalDays <- dplyr::tibble(startDay = timeWindows$startDays,
                                    endDay = timeWindows$endDays) |>
      dplyr::tibble() |>
      dplyr::distinct() |>
      dplyr::arrange(startDay)
    
    featureExtractionSettings <-
      FeatureExtraction::createTemporalCovariateSettings(
        useDemographicsGender = TRUE,
        useDemographicsAge = TRUE,
        useDemographicsAgeGroup = TRUE,
        useDemographicsRace = TRUE,
        useDemographicsEthnicity = TRUE,
        useDemographicsIndexYear = TRUE,
        useDemographicsIndexMonth = TRUE,
        useDemographicsIndexYearMonth = TRUE,
        useDemographicsPriorObservationTime = TRUE,
        useDemographicsPostObservationTime = TRUE,
        useDemographicsTimeInCohort = TRUE,
        useConditionOccurrence = TRUE,
        useProcedureOccurrence = TRUE,
        useDrugEraStart = TRUE,
        useMeasurement = TRUE,
        useConditionEraStart = TRUE,
        useConditionEraOverlap = TRUE,
        useVisitCount = TRUE,
        useVisitConceptCount = TRUE,
        useConditionEraGroupStart = FALSE,
        # do not use because https://github.com/OHDSI/FeatureExtraction/issues/144
        useConditionEraGroupOverlap = TRUE,
        useDrugExposure = FALSE,
        # leads to too many concept id
        useDrugEraOverlap = FALSE,
        useDrugEraGroupStart = FALSE,
        # do not use because https://github.com/OHDSI/FeatureExtraction/issues/144
        useDrugEraGroupOverlap = TRUE,
        useObservation = TRUE,
        useDeviceExposure = TRUE,
        useCharlsonIndex = TRUE,
        useDcsi = TRUE,
        useChads2 = TRUE,
        useChads2Vasc = TRUE,
        useHfrs = FALSE,
        temporalStartDays = feTemporalDays$startDay,
        temporalEndDays = feTemporalDays$endDay
      )
    
    return(featureExtractionSettings)
  }


#' @export
getFeatureExtractionDefaultTemporalCohortCovariateSettings <-
  function(timeWindows = OhdsiHelpers::getFeatureExtractionDefaultTimeWindows(),
           analysisId = 150,
           covariateCohortDatabaseSchema,
           covariateCohortTable,
           covariateCohorts,
           valueType = "binary") {
    feTemporalDays <- dplyr::tibble(startDay = timeWindows$startDays,
                                    endDay = timeWindows$endDays) |>
      dplyr::tibble() |>
      dplyr::distinct() |>
      dplyr::arrange(startDay)
    
    featureExtractionSettings <-
      FeatureExtraction::createCohortBasedTemporalCovariateSettings(
        analysisId = analysisId,
        covariateCohortDatabaseSchema = covariateCohortDatabaseSchema,
        covariateCohortTable = covariateCohortTable,
        covariateCohorts = covariateCohorts,
        valueType = valueType,
        temporalStartDays = feTemporalDays$startDay,
        temporalEndDays = feTemporalDays$endDay
      )
    
    return(featureExtractionSettings)
  }


getFeatureExtractionCovariateSettingDemographics <- function() {
  covariateSettings <-
    FeatureExtraction::createCovariateSettings(
      useDemographicsGender = TRUE,
      useDemographicsAge = TRUE,
      useDemographicsAgeGroup = TRUE,
      useDemographicsRace = TRUE,
      useDemographicsEthnicity = TRUE,
      useDemographicsIndexYear = TRUE,
      useDemographicsIndexMonth = TRUE,
      useDemographicsIndexYearMonth = FALSE,
      useDemographicsPriorObservationTime = TRUE,
      useDemographicsPostObservationTime = TRUE,
      useDemographicsTimeInCohort = TRUE
    )
  
  return(covariateSettings)
}




#' @export
getCovariateSettingsTimeWindows <- function(covariateSettings) {
  temporalStartDays <- covariateSettings$temporalStartDays
  temporalEndDays <- covariateSettings$temporalEndDays
  
  timeWindows <- dplyr::tibble(startDay = temporalStartDays,
                               endDay = temporalEndDays) |>
    dplyr::arrange(timeWindows)
}