#' @export
exportCohortCountAttritionFunnelTableFromWebApi <-
  function(cohortId,
           outputFolder,
           cdmSources,
           sequence = 1,
           cohortDefinitionSet = NULL,
           baseUrl = Sys.getenv("BaseUrl"),
           authMethod = "windows",
           outputExcelFileNamePrefix = "Table_1_Cohort_Attrition_") {
    # Construct file paths
    cohortResultsFile <-
      file.path(outputFolder,
                paste0("cohortResultsFromWebApi_", cohortId, ".RDS"))
    excelFile <-
      file.path(outputFolder,
                paste0(outputExcelFileNamePrefix, cohortId, ".xlsx"))
    
    # Fetch cohort results
    ROhdsiWebApi::authorizeWebApi(baseUrl = baseUrl, authMethod = authMethod)
    cohortResults <-
      ROhdsiWebApi::getCohortResults(baseUrl = baseUrl, cohortId = cohortId)
    
    # Save and read cohort results to ensure we're working with the saved data
    saveRDS(object = cohortResults, file = cohortResultsFile)
    cohortResults <- readRDS(cohortResultsFile)
    
    # Generate cohort attrition data
    s1FunnelPlotCohortAttrition <-
      OhdsiHelpers::getCohortCountAttritionWebApiGetCohortResultsOutput(
        cdmSources = cdmSources |>
          dplyr::filter(sequence %in% !!sequence),
        getCohortResultsOutput = cohortResults   
      )
    
    if (!is.null(cohortDefinitionSet)) {
      cohortName <- cohortDefinitionSet |>
        dplyr::filter(cohortId == !!cohortId) |>
        dplyr::pull(cohortName)
      
      s1FunnelPlotCohortAttrition <-
        dplyr::bind_rows(
          s1FunnelPlotCohortAttrition |>
            dplyr::select(id, name) |>
            dplyr::slice(1) |>
            dplyr::mutate(id = -2,
                          name = cohortName),
          s1FunnelPlotCohortAttrition
        )
    }
    
    # Save cohort attrition data to Excel
    OhdsiHelpers::saveDataFrameToExcel(dataFrame = s1FunnelPlotCohortAttrition, filePath = excelFile)
  }