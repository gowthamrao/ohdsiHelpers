#' @export
getCohortDiagnosticsSqlLiteConnection <- function(path) {
  connectionDetails <-
    getCohortDiagnosticsSqlLiteConnectionDetails(path = path)
  connectionHandler <-
    ResultModelManager::PooledConnectionHandler$new(connectionDetails)
  return(connectionHandler)
}
