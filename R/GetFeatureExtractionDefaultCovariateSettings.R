

#' @export
getFeatureExtractionDefaultTemporalCovariateSettings <-
  function(timeWindows = OhdsiHelpers::getFeatureExtractionDefaultTimeWindows(),
           useConditionOccurrence = TRUE,
           useProcedureOccurrence = TRUE,
           useDrugEraStart = TRUE,
           useMeasurement = TRUE,
           useConditionEraStart = TRUE,
           useConditionEraOverlap = TRUE,
           useVisitCount = TRUE,
           useVisitConceptCount = TRUE,
           useConditionEraGroupStart = TRUE,
           useConditionEraGroupOverlap = TRUE,
           useDrugExposure = FALSE,
           # leads to too many concept id
           useDrugEraOverlap = TRUE,
           useDrugEraGroupStart = TRUE,
           useDrugEraGroupOverlap = TRUE,
           useObservation = TRUE,
           useDeviceExposure = TRUE) {
    feTemporalDays <- dplyr::tibble(startDay = timeWindows$startDay,
                                    endDay = timeWindows$endDay) |>
      dplyr::tibble() |>
      dplyr::distinct() |>
      dplyr::arrange(startDay)
    
    featureExtractionSettings <-
      FeatureExtraction::createTemporalCovariateSettings(
        useConditionOccurrence = useConditionOccurrence,
        useProcedureOccurrence = useProcedureOccurrence,
        useDrugEraStart = useDrugEraStart,
        useMeasurement = useMeasurement,
        useConditionEraStart = useConditionEraStart,
        useConditionEraOverlap = useConditionEraOverlap,
        useVisitCount = useVisitCount,
        useVisitConceptCount = useVisitConceptCount,
        useConditionEraGroupStart = useConditionEraGroupStart,
        useConditionEraGroupOverlap = useConditionEraGroupOverlap,
        useDrugExposure = useDrugExposure,
        # leads to too many concept id
        useDrugEraOverlap = useDrugEraOverlap,
        useDrugEraGroupStart = useDrugEraGroupStart,
        useDrugEraGroupOverlap = useDrugEraGroupOverlap,
        useObservation = useObservation,
        useDeviceExposure = useDeviceExposure,
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
           covariateCohortDefinitionSet,
           includedCovariateIds = NULL,
           valueType = "binary") {
    feTemporalDays <- dplyr::tibble(startDay = timeWindows$startDay,
                                    endDay = timeWindows$endDay) |>
      dplyr::tibble() |>
      dplyr::distinct() |>
      dplyr::arrange(startDay)
    
    if (is.null(includedCovariateIds)) {
      includedCovariateIds <- covariateCohortDefinitionSet$cohortId
    }
    
    featureExtractionSettings <-
      FeatureExtraction::createCohortBasedTemporalCovariateSettings(
        analysisId = analysisId,
        covariateCohortDatabaseSchema = covariateCohortDatabaseSchema,
        covariateCohortTable = covariateCohortTable,
        covariateCohorts = covariateCohortDefinitionSet |>
          dplyr::select(cohortId,
                        cohortName),
        valueType = valueType,
        temporalStartDays = feTemporalDays$startDay,
        temporalEndDays = feTemporalDays$endDay,
        includedCovariateIds = includedCovariateIds
      )
    
    return(featureExtractionSettings)
  }

#' @export
getFeatureExtractionDefaultNonTimeVaryingCovariateSettings <-
  function(useDemographicsGender = TRUE,
           useDemographicsAge = TRUE,
           useDemographicsAgeGroup = TRUE,
           useDemographicsRace = TRUE,
           useDemographicsEthnicity = TRUE,
           useDemographicsIndexYear = TRUE,
           useDemographicsIndexMonth = TRUE,
           useDemographicsIndexYearMonth = FALSE,
           useDemographicsPriorObservationTime = TRUE,
           useDemographicsPostObservationTime = TRUE,
           useDemographicsTimeInCohort = TRUE,
           useCharlsonIndex = FALSE,
           useDcsi = FALSE,
           useChads2 = FALSE,
           useChads2Vasc = FALSE,
           useHfrs = FALSE) {
    covariateSettings <-
      FeatureExtraction::createCovariateSettings(
        useDemographicsGender = useDemographicsGender,
        useDemographicsAge = useDemographicsAge,
        useDemographicsAgeGroup = useDemographicsAgeGroup,
        useDemographicsRace = useDemographicsRace,
        useDemographicsEthnicity = useDemographicsEthnicity,
        useDemographicsIndexYear = useDemographicsIndexYear,
        useDemographicsIndexMonth = useDemographicsIndexMonth,
        useDemographicsIndexYearMonth = useDemographicsIndexYearMonth,
        useDemographicsPriorObservationTime = useDemographicsPriorObservationTime,
        useDemographicsPostObservationTime = useDemographicsPostObservationTime,
        useDemographicsTimeInCohort = useDemographicsTimeInCohort,
        useCharlsonIndex = useCharlsonIndex,
        useDcsi = useDcsi,
        useChads2 = useChads2,
        useChads2Vasc = useChads2Vasc,
        useHfrs = useHfrs
      )
    
    return(covariateSettings)
  }


#' @export
getCovariateSettingsTimeWindows <- function(covariateSettings) {
  timeWindows <-
    dplyr::tibble(startDay = covariateSettings$temporalStartDays,
                  endDay = covariateSettings$temporalEndDays) |>
    dplyr::left_join(getFeatureExtractionDefaultTimeWindows(),
                     by = c("startDay",
                            "endDay"))
  return(timeWindows)
}

getFeatureExtractionCovariateSettings <-
  function(covariateSettings = NULL,
           useDemographics = TRUE,
           useCovariateFeatures = TRUE,
           useCohortFeatures = TRUE,
           timeWindows = OhdsiHelpers::getFeatureExtractionDefaultTimeWindows(),
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
           useDemographicsTimeInCohort = TRUE,
           useCharlsonIndex = FALSE,
           useDcsi = FALSE,
           useChads2 = FALSE,
           useChads2Vasc = FALSE,
           useHfrs = FALSE,
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
           useDeviceExposure = TRUE) {
    
    if (is.null(covariateSettings)) {
      covariateSettings <- list()
    }
    
    if (useDemographics) {
      covariateSettingNonTimeVarying <-
        getFeatureExtractionDefaultNonTimeVaryingCovariateSettings(
          useDemographicsGender = useDemographicsGender,
          useDemographicsAge = useDemographicsAge,
          useDemographicsAgeGroup = useDemographicsAgeGroup,
          useDemographicsRace = useDemographicsRace,
          useDemographicsEthnicity = useDemographicsEthnicity,
          useDemographicsIndexYear = useDemographicsIndexYear,
          useDemographicsIndexMonth = useDemographicsIndexMonth,
          useDemographicsIndexYearMonth = useDemographicsIndexYearMonth,
          useDemographicsPriorObservationTime = useDemographicsPriorObservationTime,
          useDemographicsPostObservationTime = useDemographicsPostObservationTime,
          useDemographicsTimeInCohort = useDemographicsTimeInCohort,
          useCharlsonIndex = useCharlsonIndex,
          useDcsi = useDcsi,
          useChads2 = useChads2,
          useChads2Vasc = useChads2Vasc,
          useHfrs = useHfrs
        )
      covariateSettings <- c(covariateSettings, list(covariateSettingNonTimeVarying))
    }
    
    if (useCovariateFeatures) {
      covariateSettingsTimeVarying <- getFeatureExtractionDefaultTemporalCovariateSettings(
        timeWindows = timeWindows, 
      )
    }
    
  }