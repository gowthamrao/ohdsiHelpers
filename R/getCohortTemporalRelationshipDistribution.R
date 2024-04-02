#' @export
getCohortTemporalRelationshipDistributionInParrallel <-
  function(cdmSources,
           targetCohortTableName,
           targetCohortIds,
           targetCohortAnchorDate = "cohort_start_date",
           featureCohortIds = targetCohortIds,
           featureCohortTableName = targetCohortTableName,
           featureCohortAnchorDate = "cohort_start_date",
           databaseIds = getListOfDatabaseIds(),
           tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
           sequence = 1,
           userService = "OHDSI_USER",
           passwordService = "OHDSI_PASSWORD") {
    cdmSources <-
      getCdmSource(cdmSources = cdmSources,
                   database = databaseIds,
                   sequence = sequence)
    
    # Convert the filtered cdmSources to a list for parallel processing
    x <- list()
    for (i in 1:nrow(cdmSources)) {
      x[[i]] <- cdmSources[i,]
    }
    
    # Create a temporary directory for storing output files
    outputLocation <-
      file.path(tempfile(), paste0("c", sample(1:1000, 1)))
    dir.create(path = outputLocation,
               showWarnings = FALSE,
               recursive = TRUE)
    
    # Initialize a cluster for parallel execution
    cluster <-
      ParallelLogger::makeCluster(numberOfThreads = min(as.integer(trunc(
        parallel::detectCores() / 2
      )), length(x)))
    
    # Inner function to render and translate SQL for each CDM source
    getCohortTemporalRelationshipDistributionX <-
      function(x,
               targetCohortTableName,
               targetCohortIds,
               targetCohortAnchorDate,
               featureCohortIds,
               featureCohortTableName,
               featureCohortAnchorDate,
               tempEmulationSchema,
               outputLocation) {
        # Create connection details for each CDM source
        connectionDetails <-
          DatabaseConnector::createConnectionDetails(
            dbms = x$dbms,
            user = keyring::key_get(userService),
            password = keyring::key_get(passwordService),
            server = x$serverFinal,
            port = x$port
          )
        connection <-
          DatabaseConnector::connect(connectionDetails = connectionDetails)
        
        # Render and translate SQL for the CDM source
        output <-
          getCohortTemporalRelationshipDistribution(
            connectionDetails = connectionDetails,
            connection = connection,
            targetCohortDatabaseSchema =  x$cohortDatabaseSchemaFinal,
            targetCohortTableName = targetCohortTableName,
            targetCohortIds = targetCohortIds,
            targetCohortAnchorDate = targetCohortAnchorDate,
            featureCohortDatabaseSchema = x$cohortDatabaseSchemaFinal,
            featureCohortTableName = featureCohortTableName,
            featureCohortAnchorDate = featureCohortAnchorDate,
            tempEmulationSchema = tempEmulationSchema
          ) |>
          dplyr::tibble() |>
          dplyr::tibble(databaseKey = x$sourceKey,
                        databaseId = x$database)
        
        # Save the output to the specified location
        saveRDS(object = output,
                file = file.path(outputLocation, paste0(x$sourceKey, ".RDS")))
      }
    
    # Apply the rendering function in parallel across the cluster
    ParallelLogger::clusterApply(
      cluster = cluster,
      x = x,
      fun = getCohortTemporalRelationshipDistributionX,
      outputLocation = outputLocation,
      targetCohortTableName = targetCohortTableName,
      targetCohortIds = targetCohortIds,
      targetCohortAnchorDate = targetCohortAnchorDate,
      featureCohortIds = featureCohortIds,
      featureCohortTableName = featureCohortTableName,
      featureCohortAnchorDate = featureCohortAnchorDate,
      tempEmulationSchema = tempEmulationSchema
    )
    
    # Stop the cluster after execution
    ParallelLogger::stopCluster(cluster = cluster)
    
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
      
      unlink(x = outputLocation,
             recursive = TRUE,
             force = TRUE)
      return(outputData)
    }
    unlink(x = outputLocation,
           recursive = TRUE,
           force = TRUE)
  }


#' @export
getCohortTemporalRelationshipDistribution <-
  function(connectionDetails = NULL,
           connection = NULL,
           targetCohortDatabaseSchema,
           targetCohortTableName,
           targetCohortIds,
           targetCohortAnchorDate = "cohort_start_date",
           featureCohortIds = targetCohortIds,
           featureCohortDatabaseSchema = targetCohortDatabaseSchema,
           featureCohortTableName = targetCohortTableName,
           featureCohortAnchorDate = "cohort_start_date",
           tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
    # Validate inputs using checkmate
    checkmate::assertChoice(targetCohortAnchorDate,
                            c("cohort_start_date", "cohort_end_date"),
                            null.ok = FALSE)
    checkmate::assertChoice(featureCohortAnchorDate,
                            c("cohort_start_date", "cohort_end_date"),
                            null.ok = FALSE)
    
    checkmate::assertCharacter(targetCohortDatabaseSchema,
                               any.missing = FALSE,
                               len = 1)
    checkmate::assertCharacter(targetCohortTableName,
                               any.missing = FALSE,
                               len = 1)
    
    checkmate::assertCharacter(featureCohortDatabaseSchema,
                               any.missing = FALSE,
                               len = 1)
    checkmate::assertCharacter(featureCohortTableName,
                               any.missing = FALSE,
                               len = 1)
    
    checkmate::assertNumeric(targetCohortIds, any.missing = FALSE, lower = 1)
    checkmate::assertNumeric(featureCohortIds, any.missing = FALSE, lower = 1)
    
    if (!is.null(tempEmulationSchema)) {
      checkmate::assertCharacter(tempEmulationSchema,
                                 len = 1,
                                 any.missing = FALSE)
    }
    
    if (is.null(connection)) {
      connection <- DatabaseConnector::connect(connectionDetails)
      on.exit(DatabaseConnector::disconnect(connection))
    }
    
    sql <- "DROP TABLE IF EXISTS #target_cohort;
          DROP TABLE IF EXISTS #feature_cohort;
          DROP TABLE IF EXISTS #occurrence_next_date;
          DROP TABLE IF EXISTS #days_to_next_start;

          SELECT subject_id,
          	cohort_start_date,
          	ROW_NUMBER() OVER (
          		PARTITION BY subject_id ORDER BY cohort_start_date
          		) AS start_sequence
          INTO #target_cohort
          FROM (
          	SELECT DISTINCT subject_id,
          		@target_cohort_anchor_date cohort_start_date
          	FROM @target_cohort_database_schema.@target_cohort_table_name
          	WHERE cohort_definition_id IN (@target_cohort_definition_ids)
          	) a;

          SELECT DISTINCT subject_id,
          	@feature_cohort_anchor_date cohort_start_date
          INTO #feature_cohort
          FROM @feature_cohort_database_schema.@feature_cohort_table_name
          WHERE cohort_definition_id IN (@feature_cohort_definition_ids);

          WITH next_feature_date
          AS (
          	SELECT DISTINCT t.subject_id,
          		t.start_sequence,
          		t.cohort_start_date,
          		LEAD(f.cohort_start_date) OVER (
          			PARTITION BY t.subject_id,
          			t.cohort_start_date ORDER BY f.cohort_start_date
          			) AS next_start_date
          	FROM #target_cohort t
          	INNER JOIN #feature_cohort AS f ON t.subject_id = f.subject_id
          		AND t.cohort_start_date <= f.cohort_start_date
          	),
          next_feature_date2
          AS (
          	SELECT subject_id,
          		start_sequence,
          		cohort_start_date,
          		min(next_start_date) next_start_date
          	FROM next_feature_date
          	GROUP BY subject_id,
          		start_sequence,
          		cohort_start_date
          	)
          SELECT subject_id,
          	start_sequence,
          	cohort_start_date,
          	next_start_date,
          	DATEDIFF(day, cohort_start_date, next_start_date) AS days_to_next_cohort_start
          INTO #occurrence_next_date
          FROM next_feature_date2;

          WITH rawData (
          	start_sequence,
          	count_value
          	)
          AS (
          	SELECT start_sequence,
          		days_to_next_cohort_start AS count_value
          	FROM #occurrence_next_date

          	UNION ALL

          	SELECT - 1 AS start_sequence,
          		days_to_next_cohort_start AS count_value
          	FROM #occurrence_next_date
          	),
          overallStats (
          	start_sequence,
          	avg_value,
          	stdev_value,
          	min_value,
          	max_value,
          	total
          	)
          AS (
          	SELECT start_sequence,
          		CAST(avg(1.0 * count_value) AS FLOAT) AS avg_value,
          		CAST(stdev(count_value) AS FLOAT) AS stdev_value,
          		min(count_value) AS min_value,
          		max(count_value) AS max_value,
          		count_big(*) AS total
          	FROM rawData
          	GROUP BY start_sequence
          	),
          priorStats (
          	start_sequence,
          	count_value,
          	total,
          	accumulated
          	)
          AS (
          	SELECT start_sequence,
          		count_value,
          		count_big(*) AS total,
          		sum(count_big(*)) OVER (
          			ORDER BY count_value
          			) AS accumulated
          	FROM rawData
          	GROUP BY start_sequence,
          		count_value
          	)
          SELECT p.start_sequence,
          	o.total AS count_value,
          	o.min_value,
          	o.max_value,
          	o.avg_value,
          	o.stdev_value,
          	MIN(CASE
          			WHEN p.accumulated >= .50 * o.total
          				THEN count_value
          			ELSE o.max_value
          			END) AS median_value,
          	MIN(CASE
          			WHEN p.accumulated >= .01 * o.total
          				THEN count_value
          			ELSE o.max_value
          			END) AS p01_value,
          	MIN(CASE
          			WHEN p.accumulated >= .05 * o.total
          				THEN count_value
          			ELSE o.max_value
          			END) AS p05_value,
          	MIN(CASE
          			WHEN p.accumulated >= .10 * o.total
          				THEN count_value
          			ELSE o.max_value
          			END) AS p10_value,
          	MIN(CASE
          			WHEN p.accumulated >= .25 * o.total
          				THEN count_value
          			ELSE o.max_value
          			END) AS p25_value,
          	MIN(CASE
          			WHEN p.accumulated >= .75 * o.total
          				THEN count_value
          			ELSE o.max_value
          			END) AS p75_value,
          	MIN(CASE
          			WHEN p.accumulated >= .90 * o.total
          				THEN count_value
          			ELSE o.max_value
          			END) AS p90_value,
          	MIN(CASE
          			WHEN p.accumulated >= .95 * o.total
          				THEN count_value
          			ELSE o.max_value
          			END) AS p95_value,
          	MIN(CASE
          			WHEN p.accumulated >= .99 * o.total
          				THEN count_value
          			ELSE o.max_value
          			END) AS p99_value
          INTO #days_to_next_start
          FROM priorStats p
          INNER JOIN overallStats o ON p.start_sequence = o.start_sequence
          GROUP BY p.start_sequence,
          	o.total,
          	o.min_value,
          	o.max_value,
          	o.avg_value,
          	o.stdev_value;

          DROP TABLE IF EXISTS #target_cohort;
          DROP TABLE IF EXISTS #feature_cohort;
          DROP TABLE IF EXISTS #occurrence_next_date;
"
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = sql,
      tempEmulationSchema = tempEmulationSchema,
      target_cohort_database_schema = targetCohortDatabaseSchema,
      target_cohort_table_name = targetCohortTableName,
      target_cohort_definition_ids = targetCohortIds,
      feature_cohort_database_schema = featureCohortDatabaseSchema,
      feature_cohort_table_name = featureCohortTableName,
      feature_cohort_definition_ids = featureCohortIds,
      target_cohort_anchor_date = targetCohortAnchorDate,
      feature_cohort_anchor_date = featureCohortAnchorDate,
      progressBar = FALSE,
      profile = FALSE,
      reportOverallTime = FALSE
    )
    
    queryOutput <- DatabaseConnector::renderTranslateQuerySql(
      connection = connection,
      snakeCaseToCamelCase = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      sql = "SELECT * FROM #days_to_next_start ORDER BY start_sequence;"
    ) |>
      dplyr::tibble()
    
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = "DROP TABLE IF EXISTS #days_to_next_start;",
      tempEmulationSchema = tempEmulationSchema,
      progressBar = FALSE,
      profile = FALSE,
      reportOverallTime = FALSE
    )
    
    return(queryOutput)
  }
