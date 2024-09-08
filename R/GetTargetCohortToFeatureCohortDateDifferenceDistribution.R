#' @export
getTargetCohortToFeatureCohortDateDifferenceDistributionInParrallel <-
  function(cdmSources,
           targetCohortTableName,
           targetCohortIds,
           targetCohortAnchorDate = "cohort_start_date",
           featureCohortIds = targetCohortIds,
           featureCohortTableName = targetCohortTableName,
           featureCohortAnchorDate = "cohort_start_date",
           restrictToSameObservationPeriod = TRUE,
           databaseIds = getListOfDatabaseIds(),
           tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
           sequence = 1,
           minDays = NULL,
           maxDays = NULL,
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
               restrictToSameObservationPeriod,
               minDays,
               maxDays,
               outputLocation) {
        # Create connection details for each CDM source
        connectionDetails <- createConnectionDetails(cdmSources = x, database = x$database)
        
        connection <-
          DatabaseConnector::connect(connectionDetails = connectionDetails)
        
        # Render and translate SQL for the CDM source
        output <-
          getTargetCohortToFeatureCohortDateDifferenceDistribution(
            connectionDetails = connectionDetails,
            connection = connection,
            targetCohortDatabaseSchema =  x$cohortDatabaseSchema,
            targetCohortTableName = targetCohortTableName,
            targetCohortIds = targetCohortIds,
            targetCohortAnchorDate = targetCohortAnchorDate,
            featureCohortDatabaseSchema = x$cohortDatabaseSchema,
            featureCohortTableName = featureCohortTableName,
            featureCohortAnchorDate = featureCohortAnchorDate,
            featureCohortIds = featureCohortIds,
            restrictToSameObservationPeriod = restrictToSameObservationPeriod,
            cdmDatabaseSchema = x$cdmDatabaseSchema,
            tempEmulationSchema = tempEmulationSchema,
            minDays,
            maxDays
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
      restrictToSameObservationPeriod = restrictToSameObservationPeriod,
      tempEmulationSchema = tempEmulationSchema,
      minDays = minDays,
      maxDays = maxDays
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
getTargetCohortToFeatureCohortDateDifferenceDistribution <-
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
           cdmDatabaseSchema = NULL,
           restrictToSameObservationPeriod = TRUE,
           minDays = NULL,
           maxDays = NULL,
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
    checkmate::assertLogical(x = restrictToSameObservationPeriod,
                             any.missing = FALSE,
                             len = 1)
    
    if (restrictToSameObservationPeriod) {
      checkmate::assertCharacter(cdmDatabaseSchema,
                                 any.missing = FALSE,
                                 len = 1)
    }
    
    checkmate::assertNumeric(targetCohortIds, any.missing = FALSE, lower = 1)
    checkmate::assertNumeric(featureCohortIds, any.missing = FALSE, lower = 1)
    
    if (!is.null(minDays)) {
      checkmate::assertNumeric(minDays, any.missing = TRUE, lower = 0)
    }
    
    if (!is.null(maxDays)) {
      checkmate::assertNumeric(maxDays, any.missing = TRUE, lower = 0)
    }
    
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
            DROP TABLE IF EXISTS #target_feature;
            DROP TABLE IF EXISTS #all_next_ft_date;
            DROP TABLE IF EXISTS #days_to_next_cs_t;
            DROP TABLE IF EXISTS #next_feature_date;
            DROP TABLE IF EXISTS #agg_non_cum;
            DROP TABLE IF EXISTS #agg_cum;
            DROP TABLE IF EXISTS #aggregate;

            DROP TABLE IF EXISTS #days_to_next_start;

            ------
        		SELECT subject_id,
            	cohort_start_date,
              CAST(subject_id AS VARCHAR(25)) || '-' || CAST(cohort_start_date AS VARCHAR(10)) event_id,
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

            ------

            SELECT DISTINCT subject_id,
            	@feature_cohort_anchor_date cohort_start_date
            INTO #feature_cohort
            FROM @feature_cohort_database_schema.@feature_cohort_table_name
            WHERE cohort_definition_id IN (@feature_cohort_definition_ids);

            SELECT subject_id,
                    cohort_start_date,
                    feature_cohort
            INTO #target_feature
            FROM
            (
              SELECT subject_id,
          			cohort_start_date,
          			0 AS feature_cohort
          		FROM #target_cohort a1
          		
          		UNION
          		
          		SELECT subject_id,
          			cohort_start_date,
          			1 AS feature_cohort
          		FROM #feature_cohort a2
            ) f;
          		
            -----
           SELECT subject_id,
          	start_sequence,
          	cohort_start_date,
          	next_start_date,
          	DATEDIFF(day, cohort_start_date, next_start_date) AS days_to_next_cohort_start
          INTO #all_next_ft_date
          FROM (
          	SELECT DISTINCT t.subject_id,
          		t.start_sequence,
          		t.cohort_start_date,
          		f.feature_cohort,
          		LEAD(f.cohort_start_date) OVER (
          			PARTITION BY t.subject_id,
          			t.cohort_start_date ORDER BY f.cohort_start_date
          			) AS next_start_date
          	FROM #target_cohort t
          	INNER JOIN #target_feature AS f
          		ON t.subject_id = f.subject_id
          			AND t.cohort_start_date <= f.cohort_start_date 
          	
          {@restrict_to_same_observation_period} ? {
          	INNER JOIN @cdm_database_schema.observation_period op
          		ON t.subject_id = op.person_id
          			AND t.cohort_start_date >= op.observation_period_start_date
          			AND t.cohort_start_date <= op.observation_period_end_date
          			AND f.cohort_start_date >= op.observation_period_start_date
          			AND f.cohort_start_date <= op.observation_period_end_date 
          }
          	) f
          WHERE next_start_date IS NOT NULL
          -------------
          ORDER BY subject_id,
          	start_sequence,
          	cohort_start_date,
          	next_start_date;

            ------
            with days_to_next_cohort_start_tm as
              (
                SELECT distinct days_to_next_cohort_start
                FROM #all_next_ft_date
                WHERE days_to_next_cohort_start IS NOT NULL
              )
            select a.days_to_next_cohort_start days_to_next_cohort_start_group,
                    b.days_to_next_cohort_start
            INTO #days_to_next_cs_t
            FROM days_to_next_cohort_start_tm a
            CROSS JOIN
                days_to_next_cohort_start_tm b
             WHERE a.days_to_next_cohort_start >= b.days_to_next_cohort_start
            ORDER BY a.days_to_next_cohort_start, b.days_to_next_cohort_start;

            ------
            SELECT subject_id,
              	cohort_start_date,
              	start_sequence,
              	next_start_date,
              	CAST(subject_id AS VARCHAR(25)) || '-' || CAST(next_start_date AS VARCHAR(10)) event_id,
                DATEDIFF(day, cohort_start_date, next_start_date) AS days_to_next_cohort_start
            INTO #next_feature_date
            FROM
            (
              SELECT subject_id,
              	start_sequence,
              	cohort_start_date,
              	min(next_start_date) next_start_date
              FROM #all_next_ft_date
              GROUP BY subject_id,
              	start_sequence,
              	cohort_start_date
            ) f
            ORDER BY subject_id,
              	cohort_start_date,
              	start_sequence,
              	next_start_date;

            ------
            SELECT start_sequence,
                  days_to_next_cohort_start,
                  COUNT(DISTINCT subject_id) AS subjects,
                  COUNT(DISTINCT event_id) AS events
            INTO #agg_non_cum
            FROM #next_feature_date f
            WHERE start_sequence >= 0
            {@min_days_is_not_null} ? {
              AND days_to_next_cohort_start >= @min_days
            }
            {@max_days_is_not_null} ? {
              AND days_to_next_cohort_start <= @max_days
            }
            GROUP BY start_sequence,
                  days_to_next_cohort_start;

            ------
            SELECT start_sequence,
                  days_to_next_cohort_start_group days_to_next_cohort_start,
                  COUNT(DISTINCT subject_id) AS subjects_cum
            INTO #agg_cum
            FROM #next_feature_date nf
            INNER JOIN
                #days_to_next_cs_t ss
            ON nf.days_to_next_cohort_start = ss.days_to_next_cohort_start
            WHERE start_sequence >= 0
            {@min_days_is_not_null} ? {
              AND nf.days_to_next_cohort_start >= @min_days
            }
            {@max_days_is_not_null} ? {
              AND nf.days_to_next_cohort_start <= @max_days
            }
            GROUP BY start_sequence,
                  days_to_next_cohort_start_group;

            ------
            SELECT anc.start_sequence,
                    anc.days_to_next_cohort_start,
                    subjects,
                    events,
                    subjects_cum subjects_cumulative
            INTO #aggregated
            FROM #agg_non_cum anc
            INNER JOIN #agg_cum ac
            ON anc.start_sequence = ac.start_sequence AND
              anc.days_to_next_cohort_start = ac.days_to_next_cohort_start
            ORDER BY anc.start_sequence,
                    anc.days_to_next_cohort_start;

            ------

          SELECT *
          INTO #days_to_next_start
          FROM (
          	SELECT - 2 AS start_sequence,
          		- 2 AS days_to_next_cohort_start,
          		count(DISTINCT subject_id) AS subjects,
          		count(DISTINCT CAST(subject_id AS VARCHAR(25)) || '-' || CAST(cohort_start_date AS VARCHAR(10))) AS events,
          		count(DISTINCT subject_id) AS subjects_cumulative
          	FROM #target_cohort

          	UNION
          	SELECT start_sequence,
          		- 2 AS days_to_next_cohort_start,
          		count(DISTINCT subject_id) AS subjects,
          		count(DISTINCT CAST(subject_id AS VARCHAR(25)) || '-' || CAST(cohort_start_date AS VARCHAR(10))) AS events,
          		count(DISTINCT subject_id) AS subjects_cumulative
          	FROM #target_cohort
          	GROUP BY start_sequence

          	UNION

          	SELECT - 1 AS start_sequence,
          		- 1 AS days_to_next_cohort_start,
          		count(DISTINCT subject_id) subjects,
          		count(DISTINCT CAST(subject_id AS VARCHAR(25)) || '-' || CAST(next_start_date AS VARCHAR(10))) events,
          		count(DISTINCT subject_id) subjects_cumulative
          	FROM #all_next_ft_date
            WHERE start_sequence >= 0
            {@min_days_is_not_null} ? {
              AND days_to_next_cohort_start >= @min_days
            }
            {@max_days_is_not_null} ? {
              AND days_to_next_cohort_start <= @max_days
            }

          	UNION

          	SELECT start_sequence,
          		days_to_next_cohort_start,
          		subjects,
          		events,
          		subjects_cumulative
          	FROM #aggregated
            WHERE start_sequence >= 0
            {@min_days_is_not_null} ? {
              AND days_to_next_cohort_start >= @min_days
            }
            {@max_days_is_not_null} ? {
              AND days_to_next_cohort_start <= @max_days
            }
          	) f
          ORDER BY start_sequence,
          	days_to_next_cohort_start,
          	subjects,
          	events;

          DROP TABLE IF EXISTS #target_cohort;
          DROP TABLE IF EXISTS #feature_cohort;
          DROP TABLE IF EXISTS #target_feature;
          DROP TABLE IF EXISTS #all_next_ft_date;
          DROP TABLE IF EXISTS #days_to_next_cs_t;
          DROP TABLE IF EXISTS #next_feature_date;
          DROP TABLE IF EXISTS #agg_non_cum;
          DROP TABLE IF EXISTS #agg_cum;
          DROP TABLE IF EXISTS #aggregate;
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
      restrict_to_same_observation_period = restrictToSameObservationPeriod,
      cdm_database_schema = cdmDatabaseSchema,
      min_days = minDays,
      max_days = maxDays,
      min_days_is_not_null = !is.null(minDays),
      max_days_is_not_null = !is.null(maxDays),
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
      dplyr::filter(daysToNextCohortStart >= 0) |>
      tidyr::crossing(
        queryOutput |>
          dplyr::filter(startSequence == -1,
                        daysToNextCohortStart == -1) |>
          dplyr::select(subjects,
                        events) |>
          dplyr::rename(subjectsTotal = subjects,
                        eventsTotal = events)
      ) |>
      dplyr::mutate(
        featureSubjectProportion = round(subjects / subjectsTotal, 4),
        featureEventsProportion = round(events / eventsTotal, 4),
        featureSubjectCumulativeProportion = round(subjectsCumulative / subjectsTotal, 4)
      ) |>
      dplyr::select(
        startSequence,
        daysToNextCohortStart,
        subjects,
        events,
        subjectsCumulative,
        featureSubjectProportion,
        featureSubjectCumulativeProportion,
        featureEventsProportion
      ) |>
      tidyr::crossing(
        queryOutput |>
          dplyr::filter(startSequence == -2,
                        daysToNextCohortStart == -2) |>
          dplyr::select(subjects,
                        events) |>
          dplyr::rename(subjectsTotal = subjects,
                        eventsTotal = events)
      )  |>
      dplyr::mutate(
        targetSubjectProportion = round(subjects / subjectsTotal, 4),
        targetEventsProportion = round(events / eventsTotal, 4),
        targetSubjectCumulativeProportion = round(subjectsCumulative / subjectsTotal, 4)
      ) |>
      dplyr::select(
        startSequence,
        daysToNextCohortStart,
        subjects,
        events,
        subjectsCumulative,
        featureSubjectProportion,
        featureSubjectCumulativeProportion,
        featureEventsProportion,
        targetSubjectProportion,
        targetSubjectCumulativeProportion,
        targetEventsProportion
      ) |>
      dplyr::arrange(startSequence,
                     daysToNextCohortStart)
    
    return(summaryReport)
  }
