#' @export
createObservedPersonsCohortInParallel <-
  function(cdmSources,
           userService = "OHDSI_USER",
           passwordService = "OHDSI_PASSWORD",
           tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
           databaseIds = getListOfDatabaseIds(),
           sequence = 1,
           cohortTableName,
           startYear = 2010,
           endYear = format(Sys.Date(), "%Y"),
           cohortIdStart = NULL,
           overRide = FALSE,
           anchorMonth = "01",
           anchorDate = "01",
           minimumPriorObservationDays = 0,
           endDateStrategy = "cohort_start_date",
           unit = "day") {
    
    cdmSources <-
      getCdmSource(cdmSources = cdmSources,
                   database = databaseIds,
                   sequence = sequence)

    x <- list()
    for (i in 1:nrow(cdmSources)) {
      x[[i]] <- cdmSources[i, ]
    }

    # use Parallel Logger to run in parallel
    cluster <-
      ParallelLogger::makeCluster(numberOfThreads = min(
        as.integer(trunc(
          parallel::detectCores() /
            2
        )),
        length(x)
      ))

    createObservedPersonsCohortX <- function(x,
                                             cohortTableName,
                                             startYear,
                                             endYear,
                                             cohortIdStart,
                                             overRide,
                                             anchorMonth,
                                             anchorDate,
                                             minimumPriorObservationDays,
                                             endDateStrategy,
                                             unit) {
      
      connectionDetails <- createConnectionDetails(cdmSources = x, database = x$database)
      connection <-
        DatabaseConnector::connect(connectionDetails = connectionDetails)

      createObservedPersonsCohort(
        connection = connection,
        cohortDatabaseSchema = x$cohortDatabaseSchemaFinal,
        cohortTableName = cohortTableName,
        cdmdatabaseSchema = x$cdmDatabaseSchemaFinal,
        startYear = startYear,
        endYear = endYear,
        cohortIdStart = cohortIdStart,
        overRide = overRide,
        anchorMonth = anchorMonth,
        anchorDate = anchorDate,
        minimumPriorObservationDays = minimumPriorObservationDays,
        endDateStrategy = endDateStrategy,
        unit = unit
      )
    }

    ParallelLogger::clusterApply(
      cluster = cluster,
      x = x,
      fun = createObservedPersonsCohortX,
      startYear = startYear,
      cohortTableName = cohortTableName,
      endYear = endYear,
      cohortIdStart = cohortIdStart,
      overRide = overRide,
      anchorMonth = anchorMonth,
      anchorDate = anchorDate,
      minimumPriorObservationDays = minimumPriorObservationDays,
      endDateStrategy = endDateStrategy,
      unit = unit,
      stopOnError = FALSE
    )
    ParallelLogger::stopCluster(cluster = cluster)
  }



#' @export
createObservedPersonsCohort <- function(connection,
                                        cohortDatabaseSchema,
                                        cohortTableName,
                                        cdmdatabaseSchema,
                                        startYear = 2010,
                                        endYear = format(Sys.Date(), "%Y"),
                                        cohortIdStart = NULL,
                                        overRide = FALSE,
                                        anchorMonth = "01",
                                        anchorDate = "01",
                                        minimumPriorObservationDays = 0,
                                        endDateStrategy = "cohort_start_date",
                                        unit = "day") {
  if (is.null(cohortIdStart)) {
    cohortIdStart <- startYear
  }

  endDaysToAdd <- 0

  if (endYear < startYear) {
    stop("end year < start year")
  }

  if (endDateStrategy == "cohort_start_date") {
    exitStrategy <- "cohort_start_date "
  } else if (endDateStrategy == "observation_period_end_date") {
    exitStrategy <- "observation_period_end_date "
  } else if (is.integer(as.integer(endDateStrategy))) {
    endDaysToAdd <- as.integer(endDateStrategy)
    if (is.null(unit)) {
      stop("unit should be day, or year, or month")
    }

    exitStrategy <- paste0(
      "CASE WHEN observation_period_end_date >= DATEADD(",
      unit,
      ", ",
      endDaysToAdd,
      ", cohort_start_date) THEN observation_period_end_date ELSE DATEADD(day, ",
      endDaysToAdd,
      "cohort_start_date) END "
    )
  } else {
    stop(paste0(endDateStrategy, " is not supported."))
  }

  calendarYearSequence <- rep(x = startYear:endYear)
  cohortDefinitionIds <-
    seq(
      from = cohortIdStart,
      by = 1,
      length.out = length(calendarYearSequence)
    )

  if (!overRide) {
    cohortCounts <- CohortGenerator::getCohortCounts(
      connection = connection,
      cohortDatabaseSchema = cohortDatabaseSchema,
      cohortTable = cohortTableName,
      cohortIds = cohortDefinitionIds
    )

    if (nrow(cohortCounts) > 0) {
      stop(
        paste0(
          "The following cohorts are found to be instantiated in the target cohort table ",
          cohortDatabaseSchema,
          ".",
          cohortTableName,
          ": ",
          paste0(cohortCounts$cohortId |> sort() |> unique(),
            collapse = ", "
          )
        )
      )
    }
  }

  sql <- "

  DELETE FROM @cohort_database_schema.@cohort_table_name
  WHERE cohort_definition_id IN (@cohort_ids_to_delete);

  -- Define a recursive CTE named calendar_cohort
  WITH RECURSIVE calendar_cohort (cohort_start_date,
                                  cohort_definition_id) AS (
      SELECT
          CAST('@start_year-@anchor_month-@anchor_date' AS DATE) AS cohort_start_date,
          CAST(@cohort_id_start AS BIGINT) AS cohort_definition_id
      UNION ALL
      -- The recursive part of the CTE (the recursive member) increments the year by 1
      SELECT
          CAST(DATEADD(year, 1, cohort_start_date) AS DATE) AS cohort_start_date,
          CAST((cohort_definition_id + 1) AS BIGINT) AS cohort_definition_id
      FROM
          calendar_cohort
      -- Continue recursion until the year of cohort_start_date is less than the current year
      WHERE
          cohort_start_date <= CAST('@end_year-@anchor_month-@anchor_date' AS DATE)
  )
  INSERT INTO @cohort_database_schema.@cohort_table_name (cohort_definition_id,
                                                          subject_id,
                                                          cohort_start_date,
                                                          cohort_end_date)
  SELECT p.cohort_definition_id,
         op.person_id subject_id,
         p.cohort_start_date,
         @exit_strategy_sql as cohort_end_date
  FROM @cdm_database_schema.observation_period op
  INNER JOIN
        calendar_cohort p
  ON DATEADD(day, -@minimum_prior_observation_days, op.observation_period_start_date) <= cohort_start_date AND
      op.observation_period_end_date >= cohort_start_date
  ORDER BY cohort_start_date;

  UPDATE STATISTICS @cohort_database_schema.@cohort_table_name;
"

  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table_name = cohortTableName,
    start_year = startYear,
    end_year = endYear,
    minimum_prior_observation_days = minimumPriorObservationDays,
    cohort_ids_to_delete = cohortDefinitionIds,
    cohort_id_start = cohortIdStart,
    anchor_month = anchorMonth,
    anchor_date = anchorDate,
    exit_strategy_sql = exitStrategy,
    cdm_database_schema = cdmdatabaseSchema
  )

  cohortDefinitionset <-
    dplyr::tibble(
      cohortId = cohortDefinitionIds,
      cohortName = paste0(
        "Persons observed on ",
        calendarYearSequence,
        "-",
        anchorMonth,
        "-",
        anchorDate,
        " with ",
        minimumPriorObservationDays,
        " prior observation days"
      )
    )

  return(invisible(cohortDefinitionset))
}
