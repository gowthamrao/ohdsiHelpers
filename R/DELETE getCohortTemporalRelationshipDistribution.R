#' #' @export
#' getCohortTemporalRelationshipDistributionInParrallel <-
#'   function(cdmSources,
#'            targetCohortTableName,
#'            targetCohortIds,
#'            targetCohortAnchorDate = "cohort_start_date",
#'            featureCohortIds = targetCohortIds,
#'            featureCohortTableName = targetCohortTableName,
#'            featureCohortAnchorDate = "cohort_start_date",
#'            databaseIds = getListOfDatabaseIds(),
#'            tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
#'            sequence = 1,
#'            userService = "OHDSI_USER",
#'            passwordService = "OHDSI_PASSWORD") {
#'     cdmSources <-
#'       getCdmSource(cdmSources = cdmSources,
#'                    database = databaseIds,
#'                    sequence = sequence)
#'     
#'     # Convert the filtered cdmSources to a list for parallel processing
#'     x <- list()
#'     for (i in 1:nrow(cdmSources)) {
#'       x[[i]] <- cdmSources[i, ]
#'     }
#'     
#'     # Create a temporary directory for storing output files
#'     outputLocation <-
#'       file.path(tempfile(), paste0("c", sample(1:1000, 1)))
#'     dir.create(path = outputLocation,
#'                showWarnings = FALSE,
#'                recursive = TRUE)
#'     
#'     # Initialize a cluster for parallel execution
#'     cluster <-
#'       ParallelLogger::makeCluster(numberOfThreads = min(as.integer(trunc(
#'         parallel::detectCores() / 2
#'       )), length(x)))
#'     
#'     # Inner function to render and translate SQL for each CDM source
#'     getCohortTemporalRelationshipDistributionX <-
#'       function(x,
#'                targetCohortTableName,
#'                targetCohortIds,
#'                targetCohortAnchorDate,
#'                featureCohortIds,
#'                featureCohortTableName,
#'                featureCohortAnchorDate,
#'                tempEmulationSchema,
#'                outputLocation) {
#'         # Create connection details for each CDM source
#'         connectionDetails <-
#'           DatabaseConnector::createConnectionDetails(
#'             dbms = x$dbms,
#'             user = keyring::key_get(userService),
#'             password = keyring::key_get(passwordService),
#'             server = x$serverFinal,
#'             port = x$port
#'           )
#'         connection <-
#'           DatabaseConnector::connect(connectionDetails = connectionDetails)
#'         
#'         # Render and translate SQL for the CDM source
#'         output <-
#'           getCohortTemporalRelationshipDistribution(
#'             connectionDetails = connectionDetails,
#'             connection = connection,
#'             targetCohortDatabaseSchema =  x$cohortDatabaseSchemaFinal,
#'             targetCohortTableName = targetCohortTableName,
#'             targetCohortIds = targetCohortIds,
#'             targetCohortAnchorDate = targetCohortAnchorDate,
#'             featureCohortDatabaseSchema = x$cohortDatabaseSchemaFinal,
#'             featureCohortTableName = featureCohortTableName,
#'             featureCohortAnchorDate = featureCohortAnchorDate,
#'             tempEmulationSchema = tempEmulationSchema
#'           ) |>
#'           dplyr::tibble() |>
#'           dplyr::tibble(databaseKey = x$sourceKey,
#'                         databaseId = x$database)
#'         
#'         # Save the output to the specified location
#'         saveRDS(object = output,
#'                 file = file.path(outputLocation, paste0(x$sourceKey, ".RDS")))
#'       }
#'     
#'     # Apply the rendering function in parallel across the cluster
#'     ParallelLogger::clusterApply(
#'       cluster = cluster,
#'       x = x,
#'       fun = getCohortTemporalRelationshipDistributionX,
#'       outputLocation = outputLocation,
#'       targetCohortTableName = targetCohortTableName,
#'       targetCohortIds = targetCohortIds,
#'       targetCohortAnchorDate = targetCohortAnchorDate,
#'       featureCohortIds = featureCohortIds,
#'       featureCohortTableName = featureCohortTableName,
#'       featureCohortAnchorDate = featureCohortAnchorDate,
#'       tempEmulationSchema = tempEmulationSchema
#'     )
#'     
#'     # Stop the cluster after execution
#'     ParallelLogger::stopCluster(cluster = cluster)
#'     
#'     # Aggregate results from all CDM sources
#'     sourceKeys <- cdmSources$sourceKey
#'     filesToRead <-
#'       list.files(
#'         path = outputLocation,
#'         pattern = ".RDS",
#'         all.files = TRUE,
#'         full.names = TRUE,
#'         recursive = TRUE,
#'         ignore.case = TRUE,
#'         include.dirs = TRUE
#'       )
#'     
#'     if (length(filesToRead) > 0) {
#'       df <-
#'         dplyr::tibble(fileNames = filesToRead) |>
#'         dplyr::filter(stringr::str_detect(string = .data$fileNames, pattern = sourceKeys))
#'       
#'       # Read and combine all outputData data
#'       outputData <- c()
#'       for (i in (1:nrow(df))) {
#'         outputData[[i]] <- readRDS(df[i,]$fileNames)
#'       }
#'       
#'       outputData <- dplyr::bind_rows(outputData)
#'       
#'       unlink(x = outputLocation,
#'              recursive = TRUE,
#'              force = TRUE)
#'       return(outputData)
#'     }
#'     unlink(x = outputLocation,
#'            recursive = TRUE,
#'            force = TRUE)
#'   }
#' 
#' 
#' #' @export
#' getCohortTemporalRelationshipDistribution <-
#'   function(connectionDetails = NULL,
#'            connection = NULL,
#'            targetCohortDatabaseSchema,
#'            targetCohortTableName,
#'            targetCohortIds,
#'            targetCohortAnchorDate = "cohort_start_date",
#'            featureCohortIds = targetCohortIds,
#'            featureCohortDatabaseSchema = targetCohortDatabaseSchema,
#'            featureCohortTableName = targetCohortTableName,
#'            featureCohortAnchorDate = "cohort_start_date",
#'            tempEmulationSchema = getOption("sqlRenderTempEmulationSchema")) {
#'     # Validate inputs using checkmate
#'     checkmate::assertChoice(targetCohortAnchorDate,
#'                             c("cohort_start_date", "cohort_end_date"),
#'                             null.ok = FALSE)
#'     checkmate::assertChoice(featureCohortAnchorDate,
#'                             c("cohort_start_date", "cohort_end_date"),
#'                             null.ok = FALSE)
#'     
#'     checkmate::assertCharacter(targetCohortDatabaseSchema,
#'                                any.missing = FALSE,
#'                                len = 1)
#'     checkmate::assertCharacter(targetCohortTableName,
#'                                any.missing = FALSE,
#'                                len = 1)
#'     
#'     checkmate::assertCharacter(featureCohortDatabaseSchema,
#'                                any.missing = FALSE,
#'                                len = 1)
#'     checkmate::assertCharacter(featureCohortTableName,
#'                                any.missing = FALSE,
#'                                len = 1)
#'     
#'     checkmate::assertNumeric(targetCohortIds, any.missing = FALSE, lower = 1)
#'     checkmate::assertNumeric(featureCohortIds, any.missing = FALSE, lower = 1)
#'     
#'     if (!is.null(tempEmulationSchema)) {
#'       checkmate::assertCharacter(tempEmulationSchema,
#'                                  len = 1,
#'                                  any.missing = FALSE)
#'     }
#'     
#'     if (is.null(connection)) {
#'       connection <- DatabaseConnector::connect(connectionDetails)
#'       on.exit(DatabaseConnector::disconnect(connection))
#'     }
#'     
#'     sql <- "DROP TABLE IF EXISTS #target_cohort;
#'           DROP TABLE IF EXISTS #feature_cohort;
#'           DROP TABLE IF EXISTS #occurrence_next_date;
#'           DROP TABLE IF EXISTS #days_to_next_start;
#' 
#'           SELECT subject_id,
#'           	cohort_start_date,
#'           	ROW_NUMBER() OVER (
#'           		PARTITION BY subject_id ORDER BY cohort_start_date
#'           		) AS start_sequence
#'           INTO #target_cohort
#'           FROM (
#'           	SELECT DISTINCT subject_id,
#'           		@target_cohort_anchor_date cohort_start_date
#'           	FROM @target_cohort_database_schema.@target_cohort_table_name
#'           	WHERE cohort_definition_id IN (@target_cohort_definition_ids)
#'           	) a;
#' 
#'           SELECT DISTINCT subject_id,
#'           	@feature_cohort_anchor_date cohort_start_date
#'           INTO #feature_cohort
#'           FROM @feature_cohort_database_schema.@feature_cohort_table_name
#'           WHERE cohort_definition_id IN (@feature_cohort_definition_ids);
#' 
#' 
#'           WITH next_feature_date
#'           AS (
#'           	SELECT DISTINCT t.subject_id,
#'           		t.start_sequence,
#'           		t.cohort_start_date,
#'           		LEAD(f.cohort_start_date) OVER (
#'           			PARTITION BY t.subject_id,
#'           			t.cohort_start_date ORDER BY f.cohort_start_date
#'           			) AS next_start_date
#'           	FROM #target_cohort t
#'           	INNER JOIN #feature_cohort AS f ON t.subject_id = f.subject_id
#'           		AND t.cohort_start_date <= f.cohort_start_date
#'           	),
#'           next_feature_date2
#'           AS (
#'           	SELECT subject_id,
#'           		start_sequence,
#'           		cohort_start_date,
#'           		min(next_start_date) next_start_date
#'           	FROM next_feature_date
#'           	GROUP BY subject_id,
#'           		start_sequence,
#'           		cohort_start_date
#'           	)
#'           SELECT subject_id,
#'           	start_sequence,
#'           	cohort_start_date,
#'           	next_start_date,
#'           	DATEDIFF(day, cohort_start_date, next_start_date) AS days_to_next_cohort_start
#'           INTO #occurrence_next_date
#'           FROM next_feature_date2;
#' 
#'           -------
#'           WITH rawData (
#'           	start_sequence,
#'           	count_value
#'           	)
#'           AS (
#'           	SELECT start_sequence,
#'           		days_to_next_cohort_start AS count_value
#'           	FROM #occurrence_next_date
#' 
#'           	UNION ALL
#' 
#'           	SELECT - 1 AS start_sequence,
#'           		days_to_next_cohort_start AS count_value
#'           	FROM #occurrence_next_date
#'           	),
#'           overallStats (
#'           	start_sequence,
#'           	avg_value,
#'           	stdev_value,
#'           	min_value,
#'           	max_value,
#'           	total
#'           	)
#'           AS (
#'           	SELECT start_sequence,
#'           		CAST(avg(1.0 * count_value) AS FLOAT) AS avg_value,
#'           		CAST(CASE WHEN stdev(count_value) IS NULL THEN 0 ELSE stdev(count_value) END AS FLOAT) AS stdev_value,
#'           		min(count_value) AS min_value,
#'           		max(count_value) AS max_value,
#'           		count_big(*) AS total
#'           	FROM rawData
#'           	GROUP BY start_sequence
#'           	),
#'           priorStats (
#'           	start_sequence,
#'           	count_value,
#'           	total,
#'           	accumulated
#'           	)
#'           AS (
#'           	SELECT start_sequence,
#'           		count_value,
#'           		count_big(*) AS total,
#'           		sum(count_big(*)) OVER (
#'           		  PARTITION BY start_sequence
#'           			ORDER BY count_value
#'           			) AS accumulated
#'           	FROM rawData
#'           	GROUP BY start_sequence,
#'           		count_value
#'           	)
#'           SELECT p.start_sequence,
#'           	o.total AS count_value,
#'           	o.min_value,
#'           	o.max_value,
#'           	o.avg_value,
#'           	o.stdev_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.0 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p00_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.01 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p01_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.02 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p02_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.03 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p03_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.04 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p04_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.05 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p05_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.06 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p06_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.07 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p07_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.08 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p08_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.09 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p09_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.1 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p10_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.11 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p11_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.12 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p12_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.13 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p13_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.14 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p14_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.15 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p15_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.16 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p16_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.17 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p17_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.18 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p18_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.19 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p19_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.2 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p20_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.21 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p21_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.22 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p22_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.23 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p23_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.24 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p24_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.25 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p25_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.26 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p26_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.27 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p27_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.28 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p28_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.29 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p29_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.3 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p30_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.31 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p31_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.32 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p32_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.33 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p33_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.34 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p34_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.35 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p35_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.36 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p36_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.37 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p37_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.38 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p38_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.39 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p39_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.4 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p40_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.41 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p41_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.42 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p42_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.43 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p43_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.44 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p44_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.45 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p45_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.46 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p46_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.47 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p47_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.48 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p48_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.49 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p49_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.5 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p50_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.51 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p51_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.52 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p52_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.53 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p53_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.54 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p54_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.55 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p55_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.56 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p56_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.57 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p57_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.58 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p58_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.59 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p59_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.6 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p60_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.61 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p61_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.62 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p62_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.63 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p63_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.64 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p64_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.65 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p65_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.66 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p66_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.67 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p67_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.68 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p68_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.69 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p69_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.7 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p70_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.71 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p71_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.72 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p72_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.73 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p73_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.74 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p74_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.75 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p75_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.76 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p76_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.77 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p77_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.78 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p78_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.79 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p79_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.8 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p80_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.81 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p81_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.82 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p82_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.83 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p83_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.84 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p84_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.85 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p85_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.86 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p86_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.87 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p87_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.88 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p88_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.89 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p89_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.9 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p90_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.91 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p91_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.92 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p92_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.93 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p93_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.94 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p94_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.95 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p95_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.96 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p96_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.97 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p97_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.98 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p98_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 0.99 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p99_value,
#'           	MIN(CASE
#'           			WHEN p.accumulated >= 1 * o.total
#'           				THEN count_value
#'           			ELSE o.max_value
#'           			END) AS p100_value
#'           INTO #days_to_next_start
#'           FROM priorStats p
#'           INNER JOIN overallStats o ON p.start_sequence = o.start_sequence
#'           WHERE o.min_value IS NOT NULL
#'           GROUP BY p.start_sequence,
#'           	o.total,
#'           	o.min_value,
#'           	o.max_value,
#'           	o.avg_value,
#'           	o.stdev_value;
#' 
#'           DROP TABLE IF EXISTS #target_cohort;
#'           DROP TABLE IF EXISTS #feature_cohort;
#'           DROP TABLE IF EXISTS #occurrence_next_date;
#' "
#'     DatabaseConnector::renderTranslateExecuteSql(
#'       connection = connection,
#'       sql = sql,
#'       tempEmulationSchema = tempEmulationSchema,
#'       target_cohort_database_schema = targetCohortDatabaseSchema,
#'       target_cohort_table_name = targetCohortTableName,
#'       target_cohort_definition_ids = targetCohortIds,
#'       feature_cohort_database_schema = featureCohortDatabaseSchema,
#'       feature_cohort_table_name = featureCohortTableName,
#'       feature_cohort_definition_ids = featureCohortIds,
#'       target_cohort_anchor_date = targetCohortAnchorDate,
#'       feature_cohort_anchor_date = featureCohortAnchorDate,
#'       progressBar = FALSE,
#'       profile = FALSE,
#'       reportOverallTime = FALSE
#'     )
#'     
#'     queryOutput <- DatabaseConnector::renderTranslateQuerySql(
#'       connection = connection,
#'       snakeCaseToCamelCase = TRUE,
#'       tempEmulationSchema = tempEmulationSchema,
#'       sql = "SELECT * FROM #days_to_next_start ORDER BY start_sequence;"
#'     ) |>
#'       dplyr::tibble()
#'     
#'     DatabaseConnector::renderTranslateExecuteSql(
#'       connection = connection,
#'       sql = "DROP TABLE IF EXISTS #days_to_next_start;",
#'       tempEmulationSchema = tempEmulationSchema,
#'       progressBar = FALSE,
#'       profile = FALSE,
#'       reportOverallTime = FALSE
#'     )
#'     
#'     return(queryOutput)
#'   }
