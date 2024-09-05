#' @export
createConnectionDetails <- function() {
  keyringName <- "ohda"
  
  connectionDetails = DatabaseConnector::createConnectionDetails(
    dbms = "spark",
    connectionString = keyring::key_get("dataBricksConnectionString", keyring = keyringName),
    user = keyring::key_get("dataBricksUserName", keyring = keyringName),
    password = keyring::key_get("dataBricksPassword", keyring = keyringName)
  )
  
  return(connectionDetails)
}
