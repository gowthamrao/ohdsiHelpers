#' @export
createObservedOnDateCohortDefinitionSet <-
  function(calendarDates = dplyr::tibble(
    observedDate = c(2010:2030) |>
      as.character() |>
      paste0("-01-01"),
    priorObservationPeriod = 365,
    cohortId = (c(2010:2030) * 10000) + 101
  )) {
    observedByCalendarYear <-
      calendarDates |>
      dplyr::mutate(
        cohortName = paste0(
          "Observerd on ",
          observedDate,
          " with minimum ",
          priorObservationPeriod,
          " days prior observation"
        ),
        sql = OhdsiHelpers::getDummyCohortDefinitionSet()$sql,
        json = OhdsiHelpers::getDummyCohortDefinitionSet()$json,
        observedOnDateCohort = 1
      ) |>
      dplyr::mutate(
        sql = paste0(
          "DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = ",
          cohortId,
          " ;
              INSERT INTO @target_database_schema.@target_cohort_table
                (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
          select ",
          cohortId,
          " cohort_definition_id, person_id subject_id,
          observation_period_start_date cohort_start_date,
                    observation_period_end_date cohort_end_date
          FROM @cdm_database_schema.observation_period
          WHERE '",
          .data$observedDate,
          "' >= observation_period_start_date
          AND '",
          .data$observedDate,
          "' <= observation_period_end_Date;"
        )
      )
    
    return(observedByCalendarYear)
  }
