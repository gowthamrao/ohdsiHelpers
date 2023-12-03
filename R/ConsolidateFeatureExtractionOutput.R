# Copyright 2023 Observational Health Data Sciences and Informatics
#
# This file is part of OhdsiHelpers
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#' @export
consolidateFeatureExtractionOutput <- function(rootFolder,
                                               groupName = "databaseId",
                                               outputFolder = file.path(
                                                 rootFolder,
                                                 "combined"
                                               ),
                                               overwrite = TRUE) {
  if (!overwrite) {
    if (length(list.files(outputFolder)) > 0) {
      stop("outputFolder exists and contains files. stopping.")
    }
  }

  eligibleFiles <-
    listEligibleFiles(
      path = rootFolder,
      name = "FeatureExtraction"
    ) |>
    dplyr::filter(folderName != basename(outputFolder))

  analysisRef <- c()
  covariateRef <- c()
  covariates <- c()
  timeRef <- c()
  covariateContinous <- c()

  for (i in (1:nrow(eligibleFiles))) {
    eligibleFile <- eligibleFiles[i, ]
    data <- readRDS(eligibleFile$fullName)

    analysisRef[[i]] <- data$analysisRef
    if (!is.null(data$analysisRef)) {
      analysisRef[[i]][[groupName]] <- eligibleFile$folderName
    }

    covariateRef[[i]] <- data$covariateRef
    if (!is.null(data$covariateRef)) {
      covariateRef[[i]][[groupName]] <- eligibleFile$folderName
    }

    timeRef[[i]] <- data$timeRef
    if (!is.null(data$timeRef)) {
      timeRef[[i]][[groupName]] <- eligibleFile$folderName
    }

    covariates[[i]] <- data$covariates
    if (!is.null(data$covariates)) {
      covariates[[i]][[groupName]] <-
        eligibleFile$folderName
    }

    covariateContinous[[i]] <- data$covariateContinous
    if (!is.null(covariateContinous[[i]])) {
      covariateContinous[[i]][[groupName]] <-
        eligibleFile$folderName
    }
  }

  analysisRef <- dplyr::bind_rows(analysisRef) |>
    dplyr::distinct()
  covariateRef <- dplyr::bind_rows(covariateRef) |>
    dplyr::distinct()
  covariates <-
    dplyr::bind_rows(covariates) |>
    dplyr::distinct()
  timeRef <-
    dplyr::bind_rows(timeRef) |>
    dplyr::distinct()
  covariateContinous <-
    dplyr::bind_rows(covariateContinous) |>
    dplyr::distinct()

  featureExtractionOutput <- c()
  featureExtractionOutput$analysisRef <- analysisRef
  featureExtractionOutput$covariateRef <- covariateRef
  featureExtractionOutput$covariates <-
    covariates
  featureExtractionOutput$timeRef <-
    timeRef
  featureExtractionOutput$covariateContinous <-
    covariateContinous

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
      object = featureExtractionOutput,
      file = file.path(
        outputFolder,
        "FeatureExtraction.RDS"
      )
    )
  }

  return(featureExtractionOutput)
}
