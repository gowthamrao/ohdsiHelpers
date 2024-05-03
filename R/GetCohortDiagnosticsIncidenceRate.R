# Function to calculate cohort diagnostics incidence rate
#' @export
getCohortDiagnosticsIncidenceRate <-
  function(connectionDetails = NULL,
           connection = NULL,
           cohortDatabaseSchema,
           cohortDefinitionId = NULL,
           cdmDatabaseSchema,
           tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
           cohortTableName) {
    if (is.null(connection)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    }
    
    sql <- "
  {DEFAULT @washout_period = 365}
  {DEFAULT @first_occurrence_only = TRUE}


  IF OBJECT_ID('tempdb..#numerator', 'U') IS NOT NULL
    DROP TABLE #numerator;

  SELECT YEAR(cohort_start_date) AS calendar_year,
  	FLOOR((YEAR(cohort_start_date) - year_of_birth) / 10) AS age_group,
  	gender_concept_id,
  	COUNT(*) AS cohort_count
  INTO #numerator
  FROM (
  {@first_occurrence_only} ? {
  	SELECT subject_id,
  		MIN(cohort_start_date) AS cohort_start_date,
  		MIN(cohort_end_date) AS cohort_end_date
  	FROM @cohort_database_schema.@cohort_table
  	WHERE cohort_definition_id = @cohort_id
  	GROUP BY subject_id
   } : {
  	SELECT *
  	FROM @cohort_database_schema.@cohort_table
  	WHERE cohort_definition_id = @cohort_id
   }
  	) cohort
  INNER JOIN @cdm_database_schema.person
  	ON subject_id = person.person_id
  INNER JOIN @cdm_database_schema.observation_period
  	ON observation_period.person_id = person.person_id
  		AND DATEADD(DAY, @washout_period, observation_period_start_date) <= cohort_start_date
  		AND observation_period_end_date >= cohort_start_date
  GROUP BY YEAR(cohort_start_date),
  	FLOOR((YEAR(cohort_start_date) - year_of_birth) / 10),
  	gender_concept_id;

  IF OBJECT_ID('tempdb..#denominator', 'U') IS NOT NULL
    DROP TABLE #denominator;

  SELECT calendar_year,
  	age_group,
  	gender_concept_id,
  	SUM(CAST(DATEDIFF(DAY, start_date, end_date) AS BIGINT)) / 365.25 AS person_years
  INTO #denominator
  FROM (
  {@first_occurrence_only} ? {
  	SELECT calendar_year,
  		age_group,
  		gender_concept_id,
  		CASE
  			WHEN cohort_start_date IS NOT NULL THEN
  				CASE
  					WHEN cohort_start_date < start_date THEN end_date
  					ELSE cohort_start_date
  				END
  			ELSE start_date
  		END AS start_date,
  		end_date
  	FROM (
  }
  		SELECT person.person_id,
  			calendar_year,
  			FLOOR((calendar_year - year_of_birth) / 10) AS age_group,
  			gender_concept_id,
  			CASE
  				WHEN observation_period_start_date > DATEFROMPARTS(calendar_year, 1, 1) THEN observation_period_start_date
  				ELSE DATEFROMPARTS(calendar_year, 1, 1)
  			END AS start_date,
  			CASE
  				WHEN observation_period_end_date < DATEFROMPARTS(calendar_year + 1, 1, 1) THEN observation_period_end_date
  				ELSE DATEFROMPARTS(calendar_year + 1, 1, 1)
  			END AS end_date
  		FROM (
  			SELECT person_id,
  				DATEADD(DAY, @washout_period, observation_period_start_date) AS observation_period_start_date,
  				observation_period_end_date
  			FROM @cdm_database_schema.observation_period
  			WHERE DATEADD(DAY, @washout_period, observation_period_start_date) < observation_period_end_date
  		) trunc_op
  		INNER JOIN 	#calendar_years
  			ON YEAR(observation_period_start_date) <= calendar_year
  				AND YEAR(observation_period_end_date) >= calendar_year
  		INNER JOIN @cdm_database_schema.person
  			ON trunc_op.person_id = person.person_id
  {@first_occurrence_only} ? {
  		) time_spans_1
  	LEFT JOIN (
  		SELECT subject_id,
  			MIN(cohort_start_date) AS cohort_start_date
  		FROM @cohort_database_schema.@cohort_table
  		WHERE cohort_definition_id = @cohort_id
  		GROUP BY subject_id
  		) cohort
  	ON subject_id = person_id
  		AND cohort_start_date < end_date
  }
  	) time_spans_2
  GROUP BY calendar_year,
  	age_group,
  	gender_concept_id;

  IF OBJECT_ID('tempdb..#rates_summary', 'U') IS NOT NULL
    DROP TABLE #rates_summary;

  SELECT denominator.calendar_year,
  	denominator.age_group,
  	concept_name AS gender,
  	CASE
  		WHEN numerator.cohort_count IS NOT NULL THEN numerator.cohort_count
  		ELSE CAST(0 AS INT)
  	END AS cohort_count,
  	person_years
  INTO #rates_summary
  FROM #denominator denominator
  INNER JOIN @vocabulary_database_schema.concept
  	ON denominator.gender_concept_id = concept_id
  LEFT JOIN #numerator numerator
  	ON denominator.calendar_year = numerator.calendar_year
  		AND denominator.age_group = numerator.age_group
  		AND denominator.gender_concept_id = numerator.gender_concept_id;

  TRUNCATE TABLE #calendar_years;
  DROP TABLE #calendar_years;

  TRUNCATE TABLE #numerator;
  DROP TABLE #numerator;

  TRUNCATE TABLE #denominator;
  DROP TABLE #denominator;

          "
    
    sqlCalendar <-
      SqlRender::loadRenderTranslateSql(
        sqlFilename = "GetCalendarYearRange.sql",
        packageName = 'CohortDiagnostics',
        dbms = connection@dbms,
        cdm_database_schema = cdmDatabaseSchema
      )
    
    yearRange <-
      DatabaseConnector::querySql(connection, sqlCalendar, snakeCaseToCamelCase = TRUE)
    
    calendarYears <-
      dplyr::tibble(calendarYear = as.integer(seq(
        yearRange$startYear, yearRange$endYear, by = 1
      )))
    DatabaseConnector::insertTable(
      connection = connection,
      tableName = "#calendar_years",
      data = calendarYears,
      dropTableIfExists = TRUE,
      createTable = TRUE,
      tempTable = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      camelCaseToSnakeCase = TRUE
    )
    
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      cohort_database_schema = cohortDatabaseSchema,
      cdm_database_schema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohort_id = cohortDefinitionId,
      first_occurrence_only = TRUE,
      cohort_table = cohortTableName,
      vocabulary_database_schema = cdmDatabaseSchema,
      washout_period = 365
    )
    
    ratesSummary <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      sql = "SELECT * FROM #rates_summary;",
      snakeCaseToCamelCase = TRUE
    ) |>
      dplyr::tibble()
    
    
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = "DROP TABLE IF EXISTS #rates_summary;",
      cohort_database_schema = cohortDatabaseSchema,
      cdm_database_schema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
      cohort_definition_id = cohortDefinitionId,
      cohort_table = cohortTableName
    )
    
    return(ratesSummary)
  }



#' @export
getCohortDiagnosticsIncidenceRateInParallel <-
  function(cdmSources,
           cohortDefinitionId,
           cohortTableName,
           userService = "OHDSI_USER",
           passwordService = "OHDSI_PASSWORD",
           databaseIds = getListOfDatabaseIds(),
           tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
           sequence = 1) {
    cdmSources <-
      getCdmSource(cdmSources = cdmSources,
                   database = databaseIds,
                   sequence = sequence)
    
    x <- list()
    for (i in 1:nrow(cdmSources)) {
      x[[i]] <- cdmSources[i, ]
    }
    
    # Create a temporary directory for storing output files
    outputLocation <-
      file.path(tempfile(), paste0("c", sample(1:1000, 1)))
    dir.create(path = outputLocation,
               showWarnings = FALSE,
               recursive = TRUE)
    
    # use Parallel Logger to run in parallel
    cluster <-
      ParallelLogger::makeCluster(numberOfThreads = min(as.integer(trunc(
        parallel::detectCores() /
          2
      )),
      nrow(cdmSources)),
      maxCores)
    
    getCohortDiagnosticsIncidenceRateX <-
      function(x,
               cohortDefinitionId,
               cohortTableName,
               userService,
               passwordService,
               tempEmulationSchema) {
        connectionDetails <- DatabaseConnector::createConnectionDetails(
          dbms = x$dbms,
          user = keyring::key_get(userService),
          password = keyring::key_get(passwordService),
          server = x$serverFinal,
          port = x$port
        )
        
        cohortIncidence <- getCohortDiagnosticsIncidenceRate(
          connectionDetails = connectionDetails,
          cdmDatabaseSchema = x$cdmDatabaseSchemaFinal,
          cohortDatabaseSchema = x$cohortDatabaseSchemaFinal,
          cohortDefinitionId = cohortDefinitionId,
          cohortTableName = cohortTableName
        ) |>
          dplyr::tibble() |>
          dplyr::tibble(databaseKey = x$sourceKey,
                        databaseId = x$database)
        
        saveRDS(object = cohortIncidence,
                file = file.path(outputLocation, paste0(x$sourceKey, ".RDS")))
        
      }
    
    ParallelLogger::clusterApply(
      cluster = cluster,
      x = x,
      fun = getCohortDiagnosticsIncidenceRateX,
      cohortDefinitionId = cohortDefinitionId,
      cohortTableName = cohortTableName,
      userService = userService,
      passwordService = passwordService,
      tempEmulationSchema = tempEmulationSchema
    )
    
    # Aggregate results from all CDM sources
    sourceKeys <- cdmSources$sourceKey
    filesToRead <-
      list.files(
        path = outputLocation,
        pattern = ".RDS",
        all.files = TRUE,
        full.names = TRUE,
        recursive = TRUE,
        ignore.case = TRUE,
        include.dirs = TRUE
      )
    
    if (length(filesToRead) > 0) {
      df <-
        dplyr::tibble(fileNames = filesToRead) |>
        dplyr::filter(stringr::str_detect(string = .data$fileNames, pattern = sourceKeys))
      
      # Read and combine all outputData data
      outputData <- c()
      for (i in (1:nrow(df))) {
        outputData[[i]] <- readRDS(df[i, ]$fileNames)
      }
      
      outputData <- dplyr::bind_rows(outputData)
      
      return(outputData)
    }
  }
