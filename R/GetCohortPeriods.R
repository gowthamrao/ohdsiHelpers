# Function to calculate cohort periods
#' @export
getCohortPeriods <- function(connectionDetails = NULL,
                             connection = NULL,
                             cohortDatabaseSchema,
                             cohortDefinitionId = NULL,
                             cdmDatabaseSchema = NULL,
                             tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                             bulkLoad = Sys.getenv("DATABASE_CONNECTOR_BULK_UPLOAD"),
                             cohortTableName,
                             calendarTable,
                             stratifyByGender = FALSE,
                             stratfiyByAgeGroup = FALSE,
                             ageGroupStrata = 5,
                             isObservationTable = FALSE) {
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  if (!is.null(cohortDatabaseSchema)) {
    if (is.null(cohortDefinitionId)) {
      stop("cohortDatabaseSchema is not NULL, but cohortDefinitionId is NULL")
    }
  }

  if (any(
    stratifyByGender,
    stratfiyByAgeGroup,
    isObservationTable
  )) {
    if (is.null(cdmDatabaseSchema)) {
      stop("cdmDatabaseSchema is missing.")
    }
  }

  if (intersect(
    colnames(cohortTableName),
    c(
      "startDate",
      "endDate",
      "type"
    )
  ) |>
    length() > 0) {
    stop("Please check calendarTable.")
  }

  tempTableName <- uploadTempTable(
    connection = connection,
    tempEmulationSchema = tempEmulationSchema,
    bulkLoad = bulkLoad,
    data = calendarTable,
    camelCaseToSnakeCase = TRUE
  )

  if (isObservationTable) {
    cohortStartDate <- "observation_period_start_date"
    cohortEndDate <- "observation_period_end_date"
    personId <- "person_id"
    cohortTableName <- "observation_period"
    cohortDatabaseSchema <- cdmDatabaseSchema
    cohortDefinitionId <- NULL
  } else {
    cohortStartDate <- "cohort_start_date"
    cohortEndDate <- "cohort_end_date"
    personId <- "subject_id"
  }

  sql <- "
          SELECT  start_date,
                  end_date,
                  type,
                  {@stratify_by_gender} ? {p.gender_concept_id gender_concept_id,}
                  {@stratify_by_age_group} ? {
	                  FLOOR((YEAR(c.@cohort_start_date) - p.year_of_birth) / @age_group_strata) age_group,
	                 }
                  COUNT(DISTINCT c.@person_id) subject_count,
                  COUNT(DISTINCT CASE WHEN c.@cohort_start_date >= start_date and
                                            c.@cohort_start_date <= end_date
                                  THEN c.@person_id ELSE NULL END) subject_start,
                  COUNT(DISTINCT CASE WHEN c.@cohort_end_date >= start_date and
                                            c.@cohort_end_date <= end_date
                                  THEN c.@person_id ELSE NULL END) subject_end,
                  SUM(DATEDIFF(
                                dd,
                                CASE WHEN c.@cohort_start_date >= start_date THEN c.@cohort_start_date ELSE start_date END,
                                CASE WHEN c.@cohort_end_date <= end_date THEN c.@cohort_end_date ELSE end_date END
                  )) subject_days
          FROM
              @cohort_database_schema.@cohort_table_name c
          INNER JOIN
              @calendar_table ct
          ON c.@cohort_end_date >= start_date
          AND c.@cohort_start_date <= end_date
          {@stratify_by_gender_or_age} ? {
            INNER JOIN
              @cdm_database_schema.person p
            ON c.@person_id = p.person_id
          }
          {@filter_cohort} ? {
            WHERE cohort_definition_id = @cohort_definition_id
          }
          GROUP BY
                  {@stratify_by_gender} ? {p.gender_concept_id,}
                  {@stratify_by_age_group} ? {
	                  FLOOR((YEAR(c.@cohort_start_date) - p.year_of_birth) / @age_group_strata),
	                 }
                  start_date,
                  end_date,
                  type;
                "

  periods <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = sql,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table_name = cohortTableName,
    filter_cohort = !is.null(cohortDefinitionId),
    cohort_definition_id = cohortDefinitionId,
    cdm_database_schema = cdmDatabaseSchema,
    calendar_table = tempTableName,
    person_id = personId,
    cohort_start_date = cohortStartDate,
    cohort_end_date = cohortEndDate,
    snakeCaseToCamelCase = TRUE,
    tempEmulationSchema = tempEmulationSchema,
    stratify_by_gender_or_age = any(
      stratifyByGender,
      stratfiyByAgeGroup
    ),
    stratify_by_gender = stratifyByGender,
    stratify_by_age_group = stratfiyByAgeGroup,
    age_group_strata = ageGroupStrata
  ) |>
    dplyr::tibble() |>
    dplyr::arrange(
      .data$startDate,
      .data$endDate,
      .data$type
    )

  if ("ageGroup" %in% colnames(periods)) {
    periods <- periods |>
      dplyr::mutate(ageGroup = paste(
        ageGroupStrata * .data$ageGroup,
        (ageGroupStrata * .data$ageGroup) + (ageGroupStrata - 1),
        sep = "-"
      ))
  }

  if ("genderConceptId" %in% colnames(periods)) {
    periods <- periods |>
      dplyr::mutate(
        sex = dplyr::case_when(
          genderConceptId == 8507 ~ "Male",
          genderConceptId == 8532 ~ "Female",
          TRUE ~ "Other"
        )
      )
  }
  return(periods)
}
