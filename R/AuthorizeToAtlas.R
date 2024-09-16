#' Authorize to OHDSI PL Atlas
#'
#' This function authorizes access to OHDSI's Phenotype Library Atlas (PL Atlas) WebAPI using credentials stored in the keyring.
#'
#' @param keyringName A string indicating the name of the keyring where credentials are stored. Defaults to the `keyringName` environment variable.
#'
#' @return An authorization token returned by the `authorizeToWebApi` function.
#' @export
authorizeToOhdsiPlAtlas <- function(keyringName = Sys.getenv("keyringName")) {
  baseUrl <- "https://atlas-phenotype.ohdsi.org/WebAPI"
  message(paste("Authorizing to ", baseUrl))
  return(
    authorizeToWebApi(
      baseUrl = baseUrl,
      authMethod = "db",
      webApiUsername = keyring::key_get(service = "ohdsiAtlasPhenotypeUser", keyring = keyringName),
      webApiPassword = keyring::key_get(service = "ohdsiAtlasPhenotypePassword", keyring = keyringName)
    )
  )
}

#' Authorize to J&J Atlas
#'
#' This function authorizes access to J&J's Atlas WebAPI using Windows authentication and credentials stored in the keyring.
#'
#' @param keyringName A string indicating the name of the keyring where credentials are stored. Defaults to the `keyringName` environment variable.
#'
#' @return An authorization token returned by the `authorizeToWebApi` function.
#' @export
authorizeToJnjAtlas <- function(keyringName = Sys.getenv("keyringName")) {
  baseUrl <- keyring::key_get(service = "webApiUrl", keyring = Sys.getenv("keyringName"))
  message(paste("Authorizing to ", baseUrl))
  
  return(
    authorizeToWebApi(
      baseUrl = baseUrl,
      authMethod = "windows",
      webApiUsername = keyring::key_get(service = "ADMIN_USER", keyring = keyringName),
      webApiPassword = keyring::key_get(service = "ADMIN_PASSWORD", keyring = keyringName)
    )
  )
}

#' Authorize to WebAPI
#'
#' This is a helper function for authorizing access to a WebAPI using specified credentials.
#'
#' @param baseUrl The base URL of the WebAPI.
#' @param authMethod The authentication method (e.g., "db" for database, "windows" for Windows authentication).
#' @param webApiUsername The username for the WebAPI authentication.
#' @param webApiPassword The password for the WebAPI authentication.
#'
#' @return The result of the authorization process returned by `ROhdsiWebApi::authorizeWebApi`.
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

#' Attempt a Process with Retries
#'
#' This function attempts to execute a process multiple times with a specified delay between retries in case of failure.
#'
#' @param process A function representing the process to be attempted.
#' @param iteration The number of attempts to execute the process. Default is 5.
#' @param waitFor The number of seconds to wait between each attempt. Default is 10.
#'
#' @return The result of the process if successful, or `FALSE` if all attempts fail.
attemptProcess <- function(process,
                           iteration = 5,
                           waitFor = 10) {
  result <- try({
    process()
  }, silent = TRUE)
  
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
        Sys.sleep(waitFor)
      }
    } else {
      message(paste("Process succeeded with result:", result))
      break
    }
  }
}
