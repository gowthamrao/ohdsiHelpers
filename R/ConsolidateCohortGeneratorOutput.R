#' @export
consolidateCohortGeneratorOutput <- function(rootFolder,
                                             CohortGeneratorFolderName = "CohortGenerator",
                                             groupName = "databaseId",
                                             outputFolder = file.path(rootFolder, "Combined"),
                                             overwrite = TRUE) {
  pathToGetData <- getMatchingFolders(
    rootPath = rootFolder,
    matchString = CohortGeneratorFolderName
  )

  if (!overwrite) {
    if (length(list.files(outputFolder)) > 0) {
      stop("outputFolder exists and contains files. stopping.")
    }
  }

  eligibleFiles <-
    listEligibleFiles(
      path = pathToGetData,
      name = "CohortGenerator"
    ) |>
    dplyr::filter(.data$folderName != basename(pathToGetData))

  cohortCount <- c()
  cohortInclusionTable <- c()
  cohortInclusionResultTable <- c()
  cohortInclusionStatsTable <- c()
  cohortSummaryStatsTable <- c()

  for (i in (1:nrow(eligibleFiles))) {
    eligibleFile <- eligibleFiles[i, ]
    data <- readRDS(eligibleFile$fullName)

    checkIfHasValidData <- function(data) {
      (all(
        !is.null(data),
        is.data.frame(data),
        nrow(data) > 0
      ))
    }

    cohortCount[[i]] <- data$cohortCount
    if (checkIfHasValidData(data$cohortCount)) {
      cohortCount[[i]][[groupName]] <- eligibleFile$folderName
    }

    cohortInclusionTable[[i]] <- data$cohortInclusionTable
    if (checkIfHasValidData(data$cohortInclusionTable)) {
      cohortInclusionTable[[i]][[groupName]] <-
        cohortInclusionTable$folderName
    }

    cohortInclusionResultTable[[i]] <-
      data$cohortInclusionResultTable
    if (checkIfHasValidData(data$cohortInclusionResultTable)) {
      cohortInclusionResultTable[[i]][[groupName]] <-
        eligibleFile$folderName
    }

    cohortInclusionStatsTable[[i]] <- data$cohortInclusionStatsTable
    if (checkIfHasValidData(data$cohortInclusionStatsTable)) {
      cohortInclusionStatsTable[[i]][[groupName]] <-
        cohortInclusionStatsTable$folderName
    }

    cohortSummaryStatsTable[[i]] <- data$cohortSummaryStatsTable
    if (checkIfHasValidData(data$cohortSummaryStatsTable)) {
      cohortSummaryStatsTable[[i]][[groupName]] <-
        cohortInclusionStatsTable$folderName
    }
  }

  cohortCount <- dplyr::bind_rows(cohortCount) |>
    dplyr::distinct()
  cohortInclusionTable <- dplyr::bind_rows(cohortInclusionTable) |>
    dplyr::distinct()
  cohortInclusionResultTable <-
    dplyr::bind_rows(cohortInclusionResultTable) |>
    dplyr::distinct()
  cohortInclusionStatsTable <-
    dplyr::bind_rows(cohortInclusionStatsTable) |>
    dplyr::distinct()
  cohortSummaryStatsTable <-
    dplyr::bind_rows(cohortSummaryStatsTable) |>
    dplyr::distinct()

  cohortGeneratorOutput <- c()
  if (checkIfHasValidData(cohortCount)) {
    cohortGeneratorOutput$cohortCount <- cohortCount |>
      dplyr::tibble()
  }

  if (checkIfHasValidData(cohortInclusionTable)) {
    cohortGeneratorOutput$cohortInclusionTable <- cohortInclusionTable |>
      dplyr::tibble()
  }

  if (checkIfHasValidData(cohortInclusionResultTable)) {
    cohortGeneratorOutput$cohortInclusionResultTable <-
      cohortInclusionResultTable |>
      dplyr::tibble()
  }

  if (checkIfHasValidData(cohortInclusionStatsTable)) {
    cohortGeneratorOutput$cohortInclusionStatsTable <-
      cohortInclusionStatsTable |>
      dplyr::tibble()
  }

  if (checkIfHasValidData(cohortSummaryStatsTable)) {
    cohortGeneratorOutput$cohortSummaryStatsTable <-
      cohortSummaryStatsTable |>
      dplyr::tibble()
  }

  if (!is.null(outputFolder)) {
    unlink(
      x = outputFolder,
      recursive = TRUE,
      force = TRUE
    )

    dir.create(
      path = outputFolder,
      showWarnings = FALSE,
      recursive = TRUE
    )

    saveRDS(
      object = cohortGeneratorOutput,
      file = file.path(
        outputFolder,
        "CohortGenerator.RDS"
      )
    )
  }

  return(cohortGeneratorOutput)
}
