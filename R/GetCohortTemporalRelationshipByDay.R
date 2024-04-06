#' @export
getCohortTemporalRelationshipByDayInParrallel <-
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
    getCohortTemporalRelationshipByDayX <-
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
          getCohortTemporalRelationshipByDay(
            connectionDetails = connectionDetails,
            connection = connection,
            targetCohortDatabaseSchema =  x$cohortDatabaseSchemaFinal,
            targetCohortTableName = targetCohortTableName,
            targetCohortIds = targetCohortIds,
            targetCohortAnchorDate = targetCohortAnchorDate,
            featureCohortDatabaseSchema = x$cohortDatabaseSchemaFinal,
            featureCohortTableName = featureCohortTableName,
            featureCohortAnchorDate = featureCohortAnchorDate,
            featureCohortIds = featureCohortIds,
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
      fun = getCohortTemporalRelationshipByDayX,
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
getCohortTemporalRelationshipByDay <-
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
            			PARTITION BY subject_id
            			ORDER BY cohort_start_date
            			) AS start_sequence
      	  INTO #target_cohort
      		FROM (
      		        SELECT DISTINCT subject_id, @target_cohort_anchor_date cohort_start_date
      		        FROM @target_cohort_database_schema.@target_cohort_table_name
      		        WHERE cohort_definition_id IN (@target_cohort_definition_ids)
      		    ) a;

        	SELECT DISTINCT subject_id, @feature_cohort_anchor_date cohort_start_date
      	  INTO #feature_cohort
		      FROM @feature_cohort_database_schema.@feature_cohort_table_name
		      WHERE cohort_definition_id IN (@feature_cohort_definition_ids);

          WITH next_feature_date as
        	(
          	SELECT DISTINCT t.subject_id,
          		t.start_sequence,
          		t.cohort_start_date,
          		LEAD(f.cohort_start_date) OVER (
          			PARTITION BY t.subject_id,
          			t.cohort_start_date,
          			t.start_sequence
          			ORDER BY f.cohort_start_date
          			) AS next_start_date
          	FROM #target_cohort t
          	INNER JOIN #feature_cohort AS f
          		ON t.subject_id = f.subject_id
          			AND t.cohort_start_date <= f.cohort_start_date
          	),
          	next_feature_date2 AS
          	(
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
          	FROM next_feature_date2
          ;

          SELECT *
          INTO #days_to_next_start
          FROM
          (
            SELECT -2 AS start_sequence,
                  -2 AS days_to_next_cohort_start,
                	count(DISTINCT subject_id) subjects,
                	count(DISTINCT CAST(subject_id AS VARCHAR(25)) || '-' || CAST(cohort_start_date AS VARCHAR(10))) events
            FROM #target_cohort

            UNION

            SELECT start_sequence,
                  -2 AS days_to_next_cohort_start,
                	count(DISTINCT subject_id) subjects,
                	count(DISTINCT CAST(subject_id AS VARCHAR(25)) || '-' || CAST(cohort_start_date AS VARCHAR(10))) events
            FROM #target_cohort
            GROUP BY start_sequence

            UNION

            SELECT -1 AS start_sequence,
                  -1 AS days_to_next_cohort_start,
                	count(DISTINCT subject_id) subjects,
                	count(DISTINCT CAST(subject_id AS VARCHAR(25)) || '-' || CAST(next_start_date AS VARCHAR(10))) events
            FROM #occurrence_next_date

            UNION

            SELECT start_sequence,
                  -1 AS days_to_next_cohort_start,
                	count(DISTINCT subject_id) subjects,
                	count(DISTINCT CAST(subject_id AS VARCHAR(25)) || '-' || CAST(next_start_date AS VARCHAR(10))) events
            FROM #occurrence_next_date
            GROUP BY start_sequence

            UNION

            SELECT -1 start_sequence,
                  days_to_next_cohort_start,
                	count(DISTINCT subject_id) subjects,
                	count(DISTINCT CAST(subject_id AS VARCHAR(25)) || '-' || CAST(next_start_date AS VARCHAR(10))) events
            FROM #occurrence_next_date
            GROUP BY days_to_next_cohort_start

            UNION

            SELECT start_sequence,
            	days_to_next_cohort_start,
            	count(DISTINCT subject_id) subjects,
            	count(DISTINCT CAST(subject_id AS VARCHAR(25)) || '-' || CAST(next_start_date AS VARCHAR(10))) events
            FROM #occurrence_next_date
            WHERE next_start_date IS NOT NULL
            GROUP BY start_sequence,
            	days_to_next_cohort_start
          ) f
          ORDER BY start_sequence,
                    days_to_next_cohort_start,
                    subjects,
                    events;

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
      sql = "SELECT * FROM #days_to_next_start ORDER BY start_sequence, days_to_next_cohort_start;"
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
    
    summaryReport <- queryOutput |>
      dplyr::filter(daysToNextCohortStart > -1) |>
      dplyr::left_join(
        queryOutput |>
          dplyr::filter(daysToNextCohortStart == -1) |>
          dplyr::rename(subjectsTotal = subjects,
                        eventsTotal = events) |>
          dplyr::select(-daysToNextCohortStart),
        by = c("startSequence")
      ) |>
      dplyr::mutate(
        subjectProportion = subjects / subjectsTotal,
        eventsProportion = events / eventsTotal
      )
    
    summaryReport <- summaryReport |>
      dplyr::bind_rows(
        queryOutput |>
          dplyr::filter(startSequence == -1,
                        daysToNextCohortStart == -1) |>
          dplyr::mutate(
            subjectsTotal = subjects,
            eventsTotal = events,
            subjectProportion = 1.0,
            eventsProportion = 1.0
          )
      ) |>
      dplyr::arrange(startSequence,
                     daysToNextCohortStart)
    
    return(summaryReport)
  }
