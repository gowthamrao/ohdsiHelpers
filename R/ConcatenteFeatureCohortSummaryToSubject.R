#' @export
concatenteFeatureCohortSummaryToSubjectInParallel <-
  function(cdmSources,
           userService = "OHDSI_USER",
           passwordService = "OHDSI_PASSWORD",
           tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
           databaseIds = getListOfDatabaseIds(),
           sequence = 1,
           cohortTableName,
           subjectTableName,
           featureCohortIds,
           subjectStartDate,
           subjectEndDate,
           prefix = NULL) {
    cdmSources <- cdmSources |>
      dplyr::filter(.data$database %in% c(databaseIds)) |>
      dplyr::filter(.data$sequence == !!sequence)
    
    x <- list()
    for (i in 1:nrow(cdmSources)) {
      x[[i]] <- cdmSources[i, ]
    }
    
    # use Parallel Logger to run in parallel
    cluster <-
      ParallelLogger::makeCluster(numberOfThreads = min(as.integer(trunc(
        parallel::detectCores() /
          2
      )),
      length(x)))
    
    ## file logger
    loggerName <-
      paste0(
        "CR_",
        stringr::str_replace_all(
          string = Sys.time(),
          pattern = ":|-|EDT| ",
          replacement = ""
        )
      )
    
    ParallelLogger::addDefaultFileLogger(fileName = file.path(outputFolder, paste0(loggerName, ".txt")))
    
    concatenteFeatureCohortSummaryToSubjectX <- function(x,
                                                         featureCohortTableName,
                                                         featureCohortIds,
                                                         subjectTableName,
                                                         subjectStartDate,
                                                         subjectEndDate,
                                                         prefix) {
      connectionDetails <- DatabaseConnector::createConnectionDetails(
        dbms = x$dbms,
        user = keyring::key_get(userService),
        password = keyring::key_get(passwordService),
        server = x$serverFinal,
        port = x$port
      )
      connection <-
        DatabaseConnector::connect(connectionDetails = connectionDetails)
      
      for (j in (1:length(featureCohortIds))) {
        featureCohortId <- featureCohortIds[[j]]
        concatenteFeatureCohortSummaryToSubject(
          connection = connection,
          featureCohortTableName = featureCohortTableName,
          featureCohortDatabaseSchema = x$cohortDatabaseSchemaFinal,
          featureCohortIsTemp = FALSE,
          featureCohortId = featureCohortId,
          subjectTableName = subjectTableName,
          subjectTableDatabaseSchema = x$cohortDatabaseSchemaFinal,
          subjectTableIsTemp = FALSE,
          subjectStartDate = subjectStartDate,
          subjectEndDate = subjectEndDate,
          prefix = prefix
        )
      }
    }
    
    ParallelLogger::clusterApply(
      cluster = cluster,
      x = x,
      fun = concatenteFeatureCohortSummaryToSubjectX,
      featureCohortTableName = cohortTableName,
      featureCohortIds = featureCohortIds,
      subjectTableName = subjectTableName,
      subjectTableDatabaseSchema = subjectTableDatabaseSchema,
      subjectStartDate = subjectStartDate,
      subjectEndDate = subjectEndDate,
      prefix = prefix,
      stopOnError = FALSE
    )
    
    ParallelLogger::stopCluster(cluster = cluster)
  }



# Function to concatenate feature cohorts summary to subject table
#' @export
concatenteFeatureCohortSummaryToSubject <- function(connection,
                                                    featureCohortTableName,
                                                    featureCohortDatabaseSchema,
                                                    featureCohortIsTemp = FALSE,
                                                    featureCohortId,
                                                    subjectTableName,
                                                    subjectTableDatabaseSchema = NULL,
                                                    subjectTableIsTemp = FALSE,
                                                    subjectStartDate = NULL,
                                                    subjectEndDate = NULL,
                                                    prefix = NULL) {
  if (is.null(prefix)) {
    prefix = paste0("C", featureCohortId)
  }
  
  limitToSubjectDates <- FALSE
  if (any(!is.null(subjectStartDate), !is.null(subjectEndDate))) {
    limitToSubjectDates <- TRUE
  }
  
  useLowerLimitDate <- FALSE
  if (!is.null(subjectStartDate)) {
    useLowerLimitDate <- TRUE
  }
  
  useUpperLimitDate <- FALSE
  if (!is.null(subjectEndDate)) {
    useUpperLimitDate <- TRUE
  }
  
  sql <- "
    DROP TABLE IF EXISTS #concatenated_table;

    SELECT a.*,
      	        f.@prefix_fs,
      	        f.@prefix_fe,
      	        f.@prefix_fd,
      	        f.@prefix_ls,
      	        f.@prefix_le,
      	        f.@prefix_ld,
      	        f.@prefix_sn,
      	        f.@prefix_ev,
      	        CASE WHEN f.@prefix_fs IS NULL THEN 0 ELSE 1 END as @prefix_t
      	        {@limit_to_subject_dates} ? {
        	        {@use_lower_limit_date} ? {
                      {@use_upper_limit_date} ? {,
              	        f.@prefix_fs_btn,
              	        f.@prefix_fe_btn,
              	        f.@prefix_fd_btn,
              	        f.@prefix_ls_btn,
              	        f.@prefix_le_btn,
              	        f.@prefix_ld_btn,
              	        f.@prefix_sn_btn,
              	        f.@prefix_ev_btn,
      	                CASE WHEN f.@prefix_fs_btn IS NULL THEN 0 ELSE 1 END as @prefix_t_btn
                      }
        	        }
                  {@use_lower_limit_date} ? {,
              	        f.@prefix_fs_aft,
              	        f.@prefix_fe_aft,
              	        f.@prefix_fd_aft,
              	        f.@prefix_ls_aft,
              	        f.@prefix_le_aft,
              	        f.@prefix_ld_aft,
              	        f.@prefix_sn_aft,
              	        f.@prefix_ev_aft,
      	                CASE WHEN f.@prefix_fs_aft IS NULL THEN 0 ELSE 1 END as @prefix_t_aft
                    }
                  {@use_upper_limit_date} ? {,
              	        f.@prefix_fs_bf,
              	        f.@prefix_fe_bf,
              	        f.@prefix_fd_bf,
              	        f.@prefix_ls_bf,
              	        f.@prefix_le_bf,
              	        f.@prefix_ld_bf,
              	        f.@prefix_sn_bf,
              	        f.@prefix_ev_bf,
      	                CASE WHEN f.@prefix_fs_bf IS NULL THEN 0 ELSE 1 END as @prefix_t_bf
                      }
      	        }
      	INTO #concatenated_table
      	FROM {@subject_table_is_temp} ? {@subject_table_name} : {@subject_table_database_schema.@subject_table_name} a
      	LEFT JOIN
      	  (
      	    SELECT
      	        a.subject_id,
                MIN(a.cohort_start_date) AS @prefix_fs,
                MIN(a.cohort_end_date) AS @prefix_fe,
                DATEDIFF(day, MIN(a.cohort_start_date), MIN(a.cohort_end_date)) AS @prefix_fd,
                MAX(a.cohort_start_date) AS @prefix_ls,
                MAX(a.cohort_end_date) AS @prefix_le,
                DATEDIFF(day, MAX(a.cohort_start_date), MAX(a.cohort_end_date)) AS @prefix_ld,
                DATEDIFF(day, MIN(a.cohort_start_date), MAX(a.cohort_end_date)) AS @prefix_sn,
                COUNT(DISTINCT a.cohort_start_date) AS @prefix_ev

                {@limit_to_subject_dates} ? {

                      {@use_lower_limit_date} ? {

                            {@use_upper_limit_date} ? {,

                            MIN(CASE WHEN  a.cohort_start_date >= b.lower_limit_date AND
                                        a.cohort_start_date <= b.upper_limit_date
                              THEN a.cohort_start_date END) AS @prefix_fs_btn,
                            MIN(CASE WHEN a.cohort_start_date >= b.lower_limit_date AND
                                        a.cohort_start_date <= b.upper_limit_date
                              THEN a.cohort_end_date END) AS @prefix_fe_btn,
                            DATEDIFF(day, MIN(CASE WHEN a.cohort_start_date >= b.lower_limit_date AND
                                                      a.cohort_start_date <= b.upper_limit_date
                                            THEN a.cohort_start_date END),
                                          MIN(CASE WHEN a.cohort_start_date >= b.lower_limit_date AND
                                                      a.cohort_start_date <= b.upper_limit_date
                                            THEN a.cohort_end_date END)) AS @prefix_fd_btn,
                            MAX(CASE WHEN a.cohort_start_date >= b.lower_limit_date AND
                                          a.cohort_start_date <= b.upper_limit_date
                                            THEN a.cohort_start_date END) AS @prefix_ls_btn,
                            MAX(CASE WHEN a.cohort_start_date >= b.lower_limit_date AND
                                          a.cohort_start_date <= b.upper_limit_date
                                            THEN a.cohort_end_date END) AS @prefix_le_btn,
                            DATEDIFF(day, MAX(CASE WHEN a.cohort_start_date >= b.lower_limit_date AND
                                                    a.cohort_start_date <= b.upper_limit_date
                                            THEN a.cohort_start_date END),
                                          MAX(CASE WHEN a.cohort_start_date >= b.lower_limit_date AND
                                                    a.cohort_start_date <= b.upper_limit_date
                                            THEN a.cohort_end_date END)) AS @prefix_ld_btn,
                            DATEDIFF(day, MIN(CASE WHEN a.cohort_start_date >= b.lower_limit_date AND
                                                    a.cohort_start_date <= b.upper_limit_date
                                            THEN a.cohort_start_date END),
                                          MAX(CASE WHEN a.cohort_start_date >= b.lower_limit_date AND
                                                    a.cohort_start_date <= b.upper_limit_date
                                            THEN a.cohort_end_date END)) AS @prefix_sn_btn,
                            COUNT(DISTINCT CASE WHEN  a.cohort_start_date >= b.lower_limit_date AND
                                                      a.cohort_start_date <= b.upper_limit_date
                                            THEN a.cohort_start_date END) AS @prefix_ev_btn

                            }
                      }

                      {@use_lower_limit_date} ? {,
                      -- New aft_f fields
                      MIN(CASE WHEN a.cohort_start_date >= b.lower_limit_date
                               THEN a.cohort_start_date END) AS @prefix_fs_aft,
                      MIN(CASE WHEN a.cohort_start_date >= b.lower_limit_date
                               THEN a.cohort_end_date END) AS @prefix_fe_aft,
                      DATEDIFF(day, MIN(CASE WHEN a.cohort_start_date >= b.lower_limit_date
                                             THEN a.cohort_start_date END),
                                    MIN(CASE WHEN a.cohort_start_date >= b.lower_limit_date
                                             THEN a.cohort_end_date END)) AS @prefix_fd_aft,
                      MAX(CASE WHEN a.cohort_start_date >= b.lower_limit_date
                               THEN a.cohort_start_date END) AS @prefix_ls_aft,
                      MAX(CASE WHEN a.cohort_start_date >= b.lower_limit_date
                               THEN a.cohort_end_date END) AS @prefix_le_aft,
                      DATEDIFF(day, MAX(CASE WHEN a.cohort_start_date >= b.lower_limit_date
                                             THEN a.cohort_start_date END),
                                    MAX(CASE WHEN a.cohort_start_date >= b.lower_limit_date
                                             THEN a.cohort_end_date END)) AS @prefix_ld_aft,
                      DATEDIFF(day, MIN(CASE WHEN a.cohort_start_date >= b.lower_limit_date
                                             THEN a.cohort_start_date END),
                                    MAX(CASE WHEN a.cohort_start_date >= b.lower_limit_date
                                             THEN a.cohort_end_date END)) AS @prefix_sn_aft,
                      COUNT(DISTINCT CASE WHEN a.cohort_start_date >= b.lower_limit_date
                                          THEN a.cohort_start_date END) AS @prefix_ev_aft
                      }

                      {@use_upper_limit_date} ? {,
                          MIN(CASE WHEN a.cohort_start_date <= b.upper_limit_date
                                   THEN a.cohort_start_date END) AS @prefix_fs_bf,
                          MIN(CASE WHEN a.cohort_start_date <= b.upper_limit_date
                                   THEN a.cohort_end_date END) AS @prefix_fe_bf,
                          DATEDIFF(day, MIN(CASE WHEN a.cohort_start_date <= b.upper_limit_date
                                                 THEN a.cohort_start_date END),
                                        MIN(CASE WHEN a.cohort_start_date <= b.upper_limit_date
                                                 THEN a.cohort_end_date END)) AS @prefix_fd_bf,
                          MAX(CASE WHEN a.cohort_start_date <= b.upper_limit_date
                                   THEN a.cohort_start_date END) AS @prefix_ls_bf,
                          MAX(CASE WHEN a.cohort_start_date <= b.upper_limit_date
                                   THEN a.cohort_end_date END) AS @prefix_le_bf,
                          DATEDIFF(day, MAX(CASE WHEN a.cohort_start_date <= b.upper_limit_date
                                                 THEN a.cohort_start_date END),
                                        MAX(CASE WHEN a.cohort_start_date <= b.upper_limit_date
                                                 THEN a.cohort_end_date END)) AS @prefix_ld_bf,
                          DATEDIFF(day, MIN(CASE WHEN a.cohort_start_date <= b.upper_limit_date
                                                 THEN a.cohort_start_date END),
                                        MAX(CASE WHEN a.cohort_start_date <= b.upper_limit_date
                                                 THEN a.cohort_end_date END)) AS @prefix_sn_bf,
                          COUNT(DISTINCT CASE WHEN a.cohort_start_date <= b.upper_limit_date
                                              THEN a.cohort_start_date END) AS @prefix_ev_bf

                      }
                }

          	FROM {@feature_cohort_table_is_temp} ? {@cohort_table_name} : {@cohort_database_schema.@cohort_table_name} a
      {@limit_to_subject_dates} ? {
            LEFT JOIN
              (
                  SELECT subject_id
                  {@use_lower_limit_date} ? {, min(@lower_limit_date) lower_limit_date}
                  {@use_upper_limit_date} ? {, min(@upper_limit_date) upper_limit_date}
                  FROM {@subject_table_is_temp} ? {@subject_table_name} : {@subject_table_database_schema.@subject_table_name}
                  GROUP BY subject_id
              ) b
            ON a.subject_id = b.subject_id
      }
          	WHERE a.cohort_definition_id = @feature_definition_id
          	GROUP BY a.subject_id
      	  ) f
      	ON a.subject_id = f.subject_id;

  	DROP TABLE IF EXISTS {@subject_table_is_temp} ? {@subject_table_name} : {@subject_table_database_schema.@subject_table_name};


  	SELECT a.*
  	INTO {@subject_table_is_temp} ? {@subject_table_name} : {@subject_table_database_schema.@subject_table_name}
  	FROM #concatenated_table a
  	ORDER BY a.subject_id;

  	DROP TABLE IF EXISTS #concatenated_table;

"
  
  DatabaseConnector::renderTranslateExecuteSql(
    connection = connection,
    sql = sql,
    cohort_database_schema = featureCohortDatabaseSchema,
    cohort_table_name = featureCohortTableName,
    feature_definition_id = featureCohortId,
    feature_cohort_table_is_temp = featureCohortIsTemp,
    subject_table_is_temp = subjectTableIsTemp,
    subject_table_database_schema = subjectTableDatabaseSchema,
    subject_table_name = subjectTableName,
    prefix = prefix,
    limit_to_subject_dates = limitToSubjectDates,
    use_lower_limit_date = useLowerLimitDate,
    lower_limit_date = subjectStartDate,
    use_upper_limit_date = useUpperLimitDate,
    upper_limit_date = subjectEndDate
  )
}
