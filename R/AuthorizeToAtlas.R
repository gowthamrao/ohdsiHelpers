authorizeToOhdsiPlAtlas <- function(keyringName = "ohda") {
  baseUrl <- "https://atlas-phenotype.ohdsi.org/WebAPI"
  return(
    ROhdsiWebApi::authorizeWebApi(
      baseUrl = baseUrl,
      authMethod = "db",
      webApiUsername = keyring::key_get(
        service = "ohdsiAtlasPhenotypeUser",
        keyring = keyringName
      ),
      webApiPassword = keyring::key_get(
        service = "ohdsiAtlasPhenotypePassword",
        keyring = keyringName
      )
    )
  )
}

authorizedToJnjAtlas <- function(keyringName = "ohda") {
  baseUrl <- Sys.getenv("baseUrl")
  ROhdsiWebApi::authorizeWebApi(
    baseUrl = baseUrl,
    authMethod = "windows",
    keyring::key_get(service = "ADMIN_USER", keyring = keyringName),
    keyring::key_get(service = "ADMIN_PASSWORD", keyring = keyringName)
  ) # Windows authentication
}
