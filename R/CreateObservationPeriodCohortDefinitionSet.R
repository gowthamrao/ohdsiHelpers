#' @export
createObservationPeriodCohortDefinitionSet <- function() {
  observationPeriodCohortDefinitionSet <- dplyr::tibble(
    cohortId = 0,
    cohortName = "Observation Period",
    sql = OhdsiHelpers::getDummyCohortDefinitionSet()$sql,
    json = OhdsiHelpers::getDummyCohortDefinitionSet()$json
  ) |>
    dplyr::mutate(
      sql = "DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = 0;
              INSERT INTO @target_database_schema.@target_cohort_table
                (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
          select 0 cohort_definition_id,
                    person_id subject_id,
                    observation_period_start_date cohort_start_date,
                    observation_period_end_date cohort_end_date
          FROM @cdm_database_schema.observation_period;"
    ) |>
    dplyr::mutate(observationPeriod = 1)
  
  return(observationPeriodCohortDefinitionSet)
}
