#' Create the non temporal covariate settings
#'
#' @description
#' Create the non temporal covariate settings using three time windows
#'
#' @details
#' Create the non temporal covariate settings using three time windows. It uses FeatureExtraction::createCovariateSettings
#'
#' @param excludedCovariateConceptIds   A list of conceptIds to exclude from featureExtraction.  These
#'                                      should include all concept_ids that were used to define the
#'                                      xSpec model (default=NULL)
#' @param includedCovariateIds          A list of covariate IDs that should be restricted to.
#' @param includedCovariateConceptIds   A list of covariate concept IDs that should be restricted to.
#' @param addDescendantsToExclude       Should descendants of excluded concepts also be excluded?
#'                                      (default=FALSE)
#' @param startDayWindow1              The day to start time window 1 for feature extraction
#' @param endDayWindow1                The day to end time window 1 for feature extraction
#' @param startDayWindow2              The day to start time window 2 for feature extraction
#' @param endDayWindow2                The day to end time window 2 for feature extraction
#' @param startDayWindow3              The day to start time window 3 for feature extraction
#' @param endDayWindow3                The day to end time window 3 for feature extraction #'
#'
#' @export
getFeatureExtractionNonTemporalCovariateSettings <-
  function(excludedCovariateConceptIds = c(),
           includedCovariateIds = c(),
           includedCovariateConceptIds = c(),
           addDescendantsToExclude = FALSE,
           startDayWindow1 = 0,
           endDayWindow1 = 9999,
           startDayWindow2 = NULL,
           endDayWindow2 = NULL,
           startDayWindow3 = NULL,
           endDayWindow3 = NULL,
           useDemographicsGender = FALSE,
           useDemographicsAgeGroup = FALSE,
           useDemographicsRace = FALSE,
           useDemographicsEthnicity = FALSE,
           useConditionGroupEra = FALSE,
           useDrugGroupEra = FALSE,
           useProcedureOccurrence = FALSE,
           useDeviceExposureLongTerm = FALSE,
           useMeasurement = FALSE,
           useMeasurementValue = FALSE,
           useMeasurementRangeGroup = FALSE,
           useObservation = FALSE,
           useDistinctConditionCount = FALSE,
           useDistinctIngredientCount = FALSE,
           useDistinctProcedureCount = FALSE,
           useDistinctMeasurementCount = FALSE,
           useVisitCount = FALSE,
           useVisitConceptCount = FALSE,
           useDeviceExposure = FALSE) {
    covariateSettings1 <- FeatureExtraction::createCovariateSettings(
      useDemographicsGender = useDemographicsGender,
      useDemographicsAgeGroup = useDemographicsAgeGroup,
      useDemographicsRace = useDemographicsRace,
      useDemographicsEthnicity = useDemographicsEthnicity,
      useConditionGroupEraLongTerm = useConditionGroupEra,
      useDrugGroupEraLongTerm = useDrugGroupEra,
      useProcedureOccurrenceLongTerm = useProcedureOccurrence,
      useDeviceExposureLongTerm = useDeviceExposure,
      useMeasurementLongTerm = useMeasurement,
      useMeasurementValueLongTerm = useMeasurementValue,
      useMeasurementRangeGroupLongTerm = useMeasurementRangeGroup,
      useObservationLongTerm = useObservation,
      useDistinctConditionCountLongTerm = useDistinctConditionCount,
      useDistinctIngredientCountLongTerm = useDistinctIngredientCount,
      useDistinctProcedureCountLongTerm = useDistinctProcedureCount,
      useDistinctMeasurementCountLongTerm = useDistinctMeasurementCount,
      useVisitCountLongTerm = useVisitCount,
      useVisitConceptCountLongTerm = useVisitConceptCount,
      longTermStartDays = startDayWindow1,
      shortTermStartDays = startDayWindow1,
      mediumTermStartDays = startDayWindow1,
      endDays = endDayWindow1,
      includedCovariateConceptIds = c(includedCovariateConceptIds),
      addDescendantsToInclude = addDescendantsToExclude,
      excludedCovariateConceptIds = excludedCovariateConceptIds,
      addDescendantsToExclude = addDescendantsToExclude,
      includedCovariateIds = c(includedCovariateIds)
    )
    
    if (!(is.null(startDayWindow2))) {
      covariateSettings2 <- FeatureExtraction::createCovariateSettings(
        useConditionGroupEraShortTerm = useConditionGroupEra,
        useDrugGroupEraShortTerm = useDrugGroupEra,
        useProcedureOccurrenceShortTerm = useProcedureOccurrence,
        useDeviceExposureShortTerm = useDeviceExposure,
        useMeasurementShortTerm = useMeasurement,
        useMeasurementValueShortTerm = useMeasurementValue,
        useMeasurementRangeGroupShortTerm = useMeasurementRangeGroup,
        useObservationShortTerm = useObservation,
        useDistinctConditionCountShortTerm = useDistinctConditionCount,
        useDistinctIngredientCountShortTerm = useDistinctIngredientCount,
        useDistinctProcedureCountShortTerm = useDistinctProcedureCount,
        useDistinctMeasurementCountShortTerm = useDistinctMeasurementCount,
        useVisitCountShortTerm = useVisitCount,
        useVisitConceptCountShortTerm = useVisitConceptCount,
        longTermStartDays = startDayWindow2,
        shortTermStartDays = startDayWindow2,
        mediumTermStartDays = startDayWindow2,
        endDays = endDayWindow2,
        includedCovariateConceptIds = c(includedCovariateConceptIds),
        addDescendantsToInclude = addDescendantsToExclude,
        excludedCovariateConceptIds = excludedCovariateConceptIds,
        addDescendantsToExclude = addDescendantsToExclude,
        includedCovariateIds = c(includedCovariateIds)
      )
    }
    
    if (!(is.null(startDayWindow3))) {
      covariateSettings3 <- FeatureExtraction::createCovariateSettings(
        useConditionGroupEraMediumTerm = useConditionGroupEra,
        useDrugGroupEraMediumTerm = useDrugGroupEra,
        useProcedureOccurrenceMediumTerm = useProcedureOccurrence,
        useDeviceExposureMediumTerm = useDeviceExposure,
        useMeasurementMediumTerm = useMeasurement,
        useMeasurementValueMediumTerm = useMeasurementValue,
        useMeasurementRangeGroupMediumTerm = useMeasurementRangeGroup,
        useObservationMediumTerm = useObservation,
        useDistinctConditionCountMediumTerm = useDistinctConditionCount,
        useDistinctIngredientCountMediumTerm = useDistinctIngredientCount,
        useDistinctProcedureCountMediumTerm = useDistinctProcedureCount,
        useDistinctMeasurementCountMediumTerm = useDistinctMeasurementCount,
        useVisitCountMediumTerm = useVisitCount,
        useVisitConceptCountMediumTerm = useVisitConceptCount,
        longTermStartDays = startDayWindow3,
        shortTermStartDays = startDayWindow3,
        mediumTermStartDays = startDayWindow3,
        endDays = endDayWindow3,
        includedCovariateConceptIds = c(includedCovariateConceptIds),
        addDescendantsToInclude = addDescendantsToExclude,
        excludedCovariateConceptIds = excludedCovariateConceptIds,
        addDescendantsToExclude = addDescendantsToExclude,
        includedCovariateIds = c(includedCovariateIds)
      )
    }
    
    if (is.null(startDayWindow1)) {
      stop("The first time window must not be null")
    } else if (is.null(startDayWindow2) &
               is.null(startDayWindow3)) {
      covariateSettings <- list(covariateSettings1)
    } else if (!(is.null(startDayWindow2)) &
               is.null(startDayWindow3)) {
      covariateSettings <- list(covariateSettings1, covariateSettings2)
    } else if (!(is.null(startDayWindow3)) &
               is.null(startDayWindow2)) {
      covariateSettings <- list(covariateSettings1, covariateSettings3)
    } else {
      covariateSettings <-
        list(covariateSettings1,
             covariateSettings2,
             covariateSettings3)
    }
    
    return(covariateSettings)
  }
