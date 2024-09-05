#' @export
runCohortAlgebraUnionInParallel <- function(cdmSources,
                                            sequence = 1,
                                            tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                            databaseIds = getListOfDatabaseIds(),
                                            userService = "OHDSI_USER",
                                            passwordService = "OHDSI_PASSWORD",
                                            oldToNewCohortId,
                                            sourceCohortTableName,
                                            targetCohortTableName = sourceCohortTableName,
                                            purgeConflicts = TRUE,
                                            ...) {
  cdmSources <-
    getCdmSource(cdmSources = cdmSources,
                 database = databaseIds,
                 sequence = sequence)
  
  # Convert the filtered cdmSources to a list for parallel processing
  x <- list()
  for (i in 1:nrow(cdmSources)) {
    x[[i]] <- cdmSources[i,]
  }
  
  # Initialize a cluster for parallel execution
  cluster <-
    ParallelLogger::makeCluster(numberOfThreads = min(as.integer(trunc(
      parallel::detectCores() / 2
    )), length(x)))
  
  # Inner function to render and translate SQL for each CDM source
  unionCohortsX <-
    function(x,
             tempEmulationSchema,
             sourceCohortTable = sourceCohortTableName,
             targetCohortTable = targetCohortTableName,
             oldToNewCohortId = oldToNewCohortId,
             purgeConflicts = purgeConflicts) {
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
      
      CohortAlgebra::unionCohorts(
        connection = connection,
        sourceCohortDatabaseSchema = x$cohortDatabaseSchema,
        targetCohortDatabaseSchema = x$cohortDatabaseSchema,
        sourceCohortTable = sourceCohortTable,
        targetCohortTable = targetCohortTable,
        oldToNewCohortId = oldToNewCohortId, 
        purgeConflicts = purgeConflicts
      )
    }
  
  # Apply the function in parallel across the cluster
  ParallelLogger::clusterApply(
    cluster = cluster,
    x = x,
    fun = unionCohortsX,
    sourceCohortTable = sourceCohortTableName,
    targetCohortTable = targetCohortTableName,
    oldToNewCohortId = oldToNewCohortId,
    purgeConflicts = purgeConflicts
  )
}
