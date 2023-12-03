#' @export
getCohortDiagnosticsSqlLiteConnectionDetails <- function(path) {
  path <- normalizeFilePath(path)

  if (stringr::str_detect(
    string = path,
    pattern = ".sqlite"
  )) {
    if (!file.exists(path)) {
      stop(paste0("Cannot find file ", path))
    }
  } else {
    # are there any sqlLiteFilesInPath
    sqlLiteFilesInPath <- list.files(
      path = path,
      pattern = ".sqlite",
      full.names = TRUE,
      recursive = TRUE,
      ignore.case = TRUE
    )

    if (length(sqlLiteFilesInPath) == 0) {
      stop("No SQLlite file found in path")
    }

    if (length(sqlLiteFilesInPath) > 1) {
      stop(paste0(
        "More than 1 SQLlite file found in path",
        paste0(sqlLiteFilesInPath, "; ")
      ))
    }

    if (file.info(path)$isdir) {
      if (file.exists(file.path(
        path,
        "MergedCohortDiagnosticsData.sqlite"
      ))) {
        path <- file.path(
          path,
          "MergedCohortDiagnosticsData.sqlite"
        )
      } else if (file.exists(file.path(
        path,
        "Combined",
        "MergedCohortDiagnosticsData.sqlite"
      ))) {
        path <- file.path(
          path,
          "Combined",
          "MergedCohortDiagnosticsData.sqlite"
        )
      } else if (length(sqlLiteFilesInPath) == 1) {
        message(paste0(
          "Found an SQLite file and connecting to ",
          sqlLiteFilesInPath
        ))
        path <- sqlLiteFilesInPath
      } else {
        stop(paste0(
          "Please specifiy path to SQLlite, maybe it is ",
          paste0(sqlLiteFilesInPath, "; ")
        ))
      }
    }
  }

  # set up connection using ResultModelManager connection handler
  connectionDetails <-
    DatabaseConnector::createConnectionDetails(
      dbms = "sqlite",
      server = path
    )
  return(connectionDetails)
}
