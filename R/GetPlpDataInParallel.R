#' #' @export
#' getPlpDataInParallel <-
#'   function(cdmSources,
#'            outputFolder,
#'            targetCohortTableName,
#'            outcomeCohortTableName,
#'            targetCohortId,
#'            outcomeCohortId,
#'            covariateSettings,
#'            userService = "OHDSI_USER",
#'            passwordService = "OHDSI_PASSWORD",
#'            tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
#'            databaseIds = getListOfDatabaseIds(),
#'            sequence = 1,
#'            cohortIds = NULL) {
#'     cdmSources <-
#'       getCdmSource(cdmSources = cdmSources,
#'                    database = databaseIds,
#'                    sequence = sequence)
#'     
#'     x <- list()
#'     for (i in 1:nrow(cdmSources)) {
#'       x[[i]] <- cdmSources[i,]
#'     }
#'     
#'     # use Parallel Logger to run in parallel
#'     cluster <-
#'       ParallelLogger::makeCluster(numberOfThreads = min(as.integer(trunc(
#'         parallel::detectCores() /
#'           2
#'       )),
#'       length(x)))
#'     
#'     
#'     getPlpDataX <- function(x,
#'                             targetCohortTableName,
#'                             outcomeCohortTableName,
#'                             targetCohortId,
#'                             outcomeCohortId,
#'                             studyStartDate,
#'                             studyEndDate,
#'                             firstExposureOnly,
#'                             
#'                             tempEmulationSchema) {
#'       connectionDetails <- DatabaseConnector::createConnectionDetails(
#'         dbms = x$dbms,
#'         user = keyring::key_get(userService),
#'         password = keyring::key_get(passwordService),
#'         server = x$serverFinal,
#'         port = x$port
#'       )
#'       
#'       databaseDetails <-
#'         PatientLevelPrediction::createDatabaseDetails(
#'           connectionDetails = connectionDetails,
#'           cdmDatabaseSchema = x$cdmDatabaseSchema,
#'           cdmDatabaseId = x$sourceKey,
#'           cdmDatabaseName = x$sourceName,
#'           cohortDatabaseSchema = x$cohortDatabaseSchema,
#'           cohortTable = targetCohortTableName,
#'           outcomeDatabaseSchema = x$cohortDatabaseSchema,
#'           outcomeTable = outcomeCohortTableName,
#'           targetId = targetCohortId,
#'           outcomeIds = outcomeCohortId,
#'           cdmVersion = 5,
#'           tempEmulationSchema = tempEmulationSchema
#'         )
#'       
#'       restrictPlpDataSettings <-
#'         PatientLevelPrediction::createRestrictPlpDataSettings(
#'           studyStartDate = studyStartDate,
#'           studyEndDate = studyEndDate,
#'           firstExposureOnly = firstExposureOnly,
#'           washoutPeriod = washoutPeriod
#'         )
#'     }
#'     
#'     ParallelLogger::clusterApply(
#'       cluster = cluster,
#'       x = x,
#'       cohortDefinitionSet = cohortDefinitionSet,
#'       tempEmulationSchema = tempEmulationSchema,
#'       covariateSettings = covariateSettings,
#'       fun = executeCohortExplorerX,
#'       stopOnError = FALSE
#'     )
#'     
#'     ParallelLogger::stopCluster(cluster = cluster)
#'   }
