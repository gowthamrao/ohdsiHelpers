#' #' @export
#' copyCovariateDataObjects <- function(covariateData) {
#'   tempfileLocation <- tempfile()
#'   unlink(x = tempfileLocation,
#'          recursive = TRUE,
#'          force = TRUE)
#'   dir.create(path = tempfileLocation,
#'              showWarnings = FALSE,
#'              recursive = TRUE)
#'   suppressMessages(
#'     FeatureExtraction::saveCovariateData(
#'       covariateData = covariateData,
#'       file =
#'         file.path(tempfileLocation,
#'                   "covariateData1")
#'     )
#'   )
#'   FeatureExtraction::loadCovariateData(file.path(tempfileLocation,
#'                                                  "covariateData1"))
#' }
#' 
#' #' @export
#' duplicateCovariateDataObjects <- function(covariateData) {
#'   tempfileLocation <- tempfile()
#'   unlink(x = tempfileLocation,
#'          recursive = TRUE,
#'          force = TRUE)
#'   dir.create(path = tempfileLocation,
#'              showWarnings = FALSE,
#'              recursive = TRUE)
#'   
#'   covariateDataArray <- c()
#'   suppressMessages(
#'     FeatureExtraction::saveCovariateData(
#'       covariateData = covariateData,
#'       file =
#'         file.path(tempfileLocation,
#'                   "covariateData1")
#'     )
#'   )
#'   covariateData1 <-
#'     FeatureExtraction::loadCovariateData(file.path(tempfileLocation,
#'                                                    "covariateData1"))
#'   
#'   suppressMessages(
#'     FeatureExtraction::saveCovariateData(
#'       covariateData = covariateData1,
#'       file =
#'         file.path(tempfileLocation,
#'                   "covariateData2")
#'     )
#'   )
#'   covariateDataArray$covariateData2 <-
#'     FeatureExtraction::loadCovariateData(file.path(tempfileLocation,
#'                                                    "covariateData2"))
#'   covariateDataArray$covariateData1 <-
#'     FeatureExtraction::loadCovariateData(file.path(tempfileLocation,
#'                                                    "covariateData1"))
#'   
#'   covariateDataArray$path <- tempfileLocation
#'   
#'   return(covariateDataArray)
#' }