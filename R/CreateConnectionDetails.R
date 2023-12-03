#' @export
createConnectionDetails <- function(cdmSources,
                                    database = 'optum_extended_dod',
                                    sequence = 1,
                                    userName = keyring::key_get("OHDSI_USER"),
                                    password = keyring::key_get("OHDSI_PASSWORD"),
                                    ohda = FALSE) {
  cdmSource <-
    getCdmSource(cdmSources = cdmSources,
                 database = tolower(database),
                 sequence = sequence)
  
  if (nrow(cdmSource) == 0) {
    stop(paste0("database ", database, "did not match cdmSources. The expected values are: ", paste0(cdmSources$database |> unique() |> sort(), 
                                                                                                     collapse = ", ")))
  }
  
  if (!ohda) {
    connectionDetails <- DatabaseConnector::createConnectionDetails(
      dbms = cdmSource$dbms,
      user = userName,
      password = password,
      server = cdmSource$serverFinal,
      port = cdmSource$port
    )
  } else {
    connectionDetails <- DatabaseConnector::createConnectionDetails(
      dbms = cdmSource$dbms,
      user = userName,
      password = password,
      server = cdmSource$server,
      port = cdmSource$port
    )
  }
  
  return(connectionDetails)
}