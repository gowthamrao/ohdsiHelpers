

#' @export
getTable1SpecificationsRow <- function(analysisId,
                                       conceptIds = NULL,
                                       covariateIds = NULL,
                                       label = "Feature cohorts") {
  if (is.null(conceptIds) & is.null(covariateIds)) {
    stop("please provide atleast conceptIds or covariateIds")
  }
  
  if (!length(analysisId) == 1) {
    stop("only one analysis id")
  }
  
  if (!length(label) == 1) {
    stop("only one label")
  }
  
  covariateIds <- c(covariateIds,
                    (conceptIds * 1000) + analysisId) |>
    unique()
  
  output <- dplyr::tibble(
    label = label,
    analysisId = analysisId,
    covariateIds = paste0(covariateIds, collapse = ",")
  )
  return(output)
}


#' @export
getTable1SpecificationsFromCovariateData <-
  function(covariateData = NULL,
           covariateRef = NULL,
           analysisRef = NULL) {
    table1Specifications <- c()
    
    if (!is.null(covariateData)) {
      covariateRef <- covariateData$covariateRef |>
        dplyr::collect()
      
      analysisRef <- covariateData$analysisRef |>
        dplyr::collect()
    }
    
    analysisNames <- analysisRef |>
      dplyr::select(analysisId,
                    analysisName) |>
      dplyr::distinct()
    
    if (nrow(analysisNames) > 0) {
      for (i in (1:nrow(analysisNames))) {
        analysisName <-
          analysisNames[i, ]
        
        covariateIds <- covariateRef |>
          dplyr::collect() |>
          dplyr::filter(analysisId %in% analysisName$analysisId) |>
          dplyr::select(.data$covariateId) |>
          dplyr::distinct() |>
          dplyr::collect() |>
          dplyr::pull(.data$covariateId) |>  # dont sort
          unique()
        
        table1Specifications[[i]] <-
          getTable1SpecificationsRow(
            analysisId = analysisName$analysisId,
            conceptIds = NULL,
            covariateIds = covariateIds,
            label = analysisName$analysisName |> SqlRender::camelCaseToTitleCase() |> stringr::str_trim() |> stringr::str_squish()
          )
      }
      table1Specifications <-
        dplyr::bind_rows(table1Specifications)
    }
    
    return(table1Specifications)
  }