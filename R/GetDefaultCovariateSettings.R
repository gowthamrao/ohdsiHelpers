#' @export
getDefaultCovariateSettings <- function(temporalStartDays = NULL,
                                        temporalEndDays = NULL) {
  
  if (is.null(temporalStartDays)) {
        temporalStartDays = c(
          # components displayed in cohort characterization
          -9999, # anytime prior
          -9999, # anytime prior
          -365, # long term prior
          -180, # medium term prior
          -30, # short term prior
          
          # components displayed in temporal characterization
          -365, # one year prior to -31
          -30, # 30 day prior not including day 0
          0, # index date only
          1, # 1 day after to day 30
          31,
          -9999, # Any time prior to any time future
          -30,
          -60,
          -90,
          -120,
          31,
          61,
          91,
          -7,
          -1,
          seq(-1, -390, by = -30) - 30,
          seq(1, 390, by = 30) - 30,
          -365,
          1,
          seq(-1, -390, by = -30) - 30,
          rep(1, length(seq(1, 390, by = 30)))
        )
  }
  
  if (is.null(temporalEndDays)) {
        temporalEndDays = c(
          0, # anytime prior
          -1, #anytime prior not including day 0
          0, # long term prior
          0, # medium term prior
          0, # short term prior
          
          # components displayed in temporal characterization
          -31, # one year prior to -31
          -1, # 30 day prior not including day 0
          0, # index date only
          30, # 1 day after to day 30
          365,
          9999, # Any time prior to any time future
          -1,
          -31,
          -61,
          -91,
          60,
          90,
          120,
          7,
          1,
          seq(-1, -390, by = -30),
          seq(1, 390, by = 30),
          -1,
          365,
          rep(-1, length(seq(-1, -390, by = -30) - 30)),
          seq(1, 390, by = 30)
        )
  }
  
  if (length(temporalStartDays) != length(temporalEndDays)) {
    stop("length of temporal start days and end days are not the same")
  }
  
  feTemporalDays <- dplyr::tibble(
    startDays = temporalStartDays,
    endDays = temporalEndDays
  ) |> 
    dplyr::tibble() |> 
    dplyr::distinct() |> 
    dplyr::arrange(startDays)
  
  featureExtractionSettings <- FeatureExtraction::createTemporalCovariateSettings(
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
    useConditionEraGroupStart = FALSE, # do not use because https://github.com/OHDSI/FeatureExtraction/issues/144
    useConditionEraGroupOverlap = TRUE,
    useDrugExposure = FALSE, # leads to too many concept id
    useDrugEraOverlap = FALSE,
    useDrugEraGroupStart = FALSE, # do not use because https://github.com/OHDSI/FeatureExtraction/issues/144
    useDrugEraGroupOverlap = TRUE,
    useObservation = TRUE,
    useDeviceExposure = TRUE,
    useCharlsonIndex = TRUE,
    useDcsi = TRUE,
    useChads2 = TRUE,
    useChads2Vasc = TRUE,
    useHfrs = FALSE,
    temporalStartDays = feTemporalDays$startDays,
    temporalEndDays = feTemporalDays$endDays
  )
  
  return(featureExtractionSettings)
}
