#' @export
authorizeToOhdsiPlAtlas <- function(keyringName = "ohda") {
  baseUrl <- "https://atlas-phenotype.ohdsi.org/WebAPI"
  message(paste("Authorizing to ", baseUrl))
  return(
    authorizeToWebApi(
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

#' @export
authorizeToJnjAtlas <- function(keyringName = "ohda") {
  baseUrl <- Sys.getenv("baseUrl")
  message(paste("Authorizing to ", baseUrl))

  return(
    authorizeToWebApi(
      baseUrl = Sys.getenv("baseUrl"),
      authMethod = "windows",
      webApiUsername = keyring::key_get(service = "ADMIN_USER", keyring = keyringName),
      webApiPassword = keyring::key_get(service = "ADMIN_PASSWORD", keyring = keyringName)
    )
  ) # Windows authentication
}

#' @export
authorizeToWebApi <-
  function(baseUrl,
           authMethod,
           webApiUsername,
           webApiPassword) {
    process <- ROhdsiWebApi::authorizeWebApi(
      baseUrl = baseUrl,
      authMethod = authMethod,
      webApiUsername = webApiUsername,
      webApiPassword = webApiPassword
    )
    return(process)
  }

attemptProcess <- function(process,
                           iteration = 5,
                           waitFor = 10) {
  result <- try(
    {
      process()
    },
    silent = TRUE
  )

  if (class(result) == "try-error") {
    return(FALSE)
  } else {
    return(result)
  }

  for (i in 1:iteration) {
    message(paste("Attempt", i, "of ", iteration))
    result <- attemptProcess()

    if (result == FALSE) {
      if (i == iteration) {
        message("Final attempt failed. Exiting.")
        break
      } else {
        message(paste0("An error occurred. Will retry in ", waitFor, " seconds..."))
        Sys.sleep(waitFor) # Wait for seconds before next attempt
      }
    } else {
      message(paste("Process succeeded with result:", result))
      break
    }
  }
}
