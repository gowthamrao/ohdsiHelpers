# Function to calculate cohort periods
#' @export
getCohortPeriods <- function(connectionDetails = NULL,
                             connection = NULL,
                             cohortDatabaseSchema,
                             tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                             bulkLoad = Sys.getenv("DATABASE_CONNECTOR_BULK_UPLOAD"),
                             cohortTableName,
                             calendarTable,
                             isObservationTable = FALSE) {
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }
  
  if (intersect(colnames(cohortTableName),
                c("startDate",
                  "endDate",
                  "type")) |>
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
    cohortStartDate = "observation_period_start_date"
    cohortEndDate = "observation_period_end_date"
    personId = "person_id"
    cohortTableName = "observation_period"
  } else {
    cohortStartDate = "cohort_start_date"
    cohortEndDate = "cohort_end_date"
    personId = "subject_id"
  }
  
  sql <- "
          SELECT  start_date,
                  end_date,
                  type,
                  COUNT(DISTINCT @person_id) subject_count,
                  COUNT(DISTINCT CASE WHEN @cohort_start_date >= start_date and @cohort_start_date <= end_date THEN @person_id ELSE NULL END) subject_start,
                  COUNT(DISTINCT CASE WHEN @cohort_end_date >= start_date and @cohort_end_date <= end_date THEN @person_id ELSE NULL END) subject_end,
                  SUM(DATEDIFF(
                                dd,
                                CASE WHEN @cohort_start_date >= start_date THEN @cohort_start_date ELSE start_date END,
                                CASE WHEN @cohort_end_date <= end_date THEN @cohort_end_date ELSE end_date END
                  )) subject_days
          FROM
              @cohort_database_schema.@cohort_table_name c
          INNER JOIN
              @calendar_table ct
          ON @cohort_end_date >= start_date
          AND @cohort_start_date <= end_date
          GROUP BY
                  start_date,
                  end_date,
                  type;
                "
  
  periods <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = sql,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table_name = cohortTableName,
    calendar_table = tempTableName,
    person_id = personId,
    cohort_start_date = cohortStartDate,
    cohort_end_date = cohortEndDate,
    snakeCaseToCamelCase = TRUE,
    tempEmulationSchema = tempEmulationSchema
  ) |>
    dplyr::tibble() |>
    dplyr::arrange(startDate,
                   endDate,
                   type)
  return(periods)
}
