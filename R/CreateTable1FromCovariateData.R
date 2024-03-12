#' @export
checkFilterTemporalCovariateDataByTimeIdCohortId <-
  function(covariateData,
           cohortId,
           timeId,
           multipleCohortId,
           excludedCovariateIds) {
    if (FeatureExtraction::isTemporalCovariateData(covariateData)) {
      if (is.null(timeId)) {
        stop("one of given covariateData is temporal. its corresponding timeId is NULL")
      } else if (length(timeId) != 1) {
        stop("timeId can only be of length 1")
      } else {
        covariateData$timeRef <- covariateData$timeRef |>
          dplyr::filter(.data$timeId == !!timeId)
        covariateData$covariates <- covariateData$covariates |>
          dplyr::filter(.data$timeId == !!timeId) |>
          dplyr::select(-.data$timeId)
      }
    }
    
    if (is.null(cohortId)) {
      if (length(attributes(covariateData)$metaData$cohortIds) > 1) {
        stop(
          "covariateData has records for more than one cohortId. Please provide cohortId to filter"
        )
      }
    } else if (length(cohortId) != 1) {
      if (multipleCohortId) {
        warning("experimental support for multiple cohort id")
      } else {
        stop("cohortId has a length more than 1")
      }
    } else {
      covariateData$covariates <- covariateData$covariates |>
        dplyr::filter(.data$cohortDefinitionId == !!cohortId)
    }
    
    if (!is.null(excludedCovariateIds)) {
      covariateData$covariates <- covariateData$covariates |>
        dplyr::filter(!covariateId %in% excludedCovariateIds)
    }
    
    return(covariateData)
  }




#' @export
createTable1SpecificationsRow <- function(analysisId,
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
createTable1SpecificationsFromCovariateData <-
  function(covariateData) {
    table1Specifications <- c()
    
    analysisNames <- covariateData$analysisRef |>
      dplyr::collect() |>
      dplyr::select(analysisId,
                    analysisName) |>
      dplyr::distinct()
    
    if (nrow(analysisNames) > 0) {
      for (i in (1:nrow(analysisNames))) {
        analysisName <-
          analysisNames[i, ]
        
        covariateIds <- covariateData$covariateRef |>
          dplyr::collect() |>
          dplyr::filter(analysisId %in% analysisName$analysisId) |>
          dplyr::select(.data$covariateId) |>
          dplyr::distinct() |>
          dplyr::collect() |>
          dplyr::pull(.data$covariateId) |>  # dont sort
          unique()
        
        table1Specifications[[i]] <-
          createTable1SpecificationsRow(
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


#' @export
createTable1FromCovariateData <- function(covariateData1,
                                          covariateData2 = NULL,
                                          cohortId1 = NULL,
                                          cohortId2 = NULL,
                                          multipleCohortId = FALSE,
                                          timeId1 = NULL,
                                          timeId2 = NULL,
                                          table1Specifications = createTable1SpecificationsFromCovariateData(covariateData1),
                                          showCounts = FALSE,
                                          showPercent = TRUE,
                                          percentDigits = 1,
                                          valueDigits = 1,
                                          stdDiffDigits = 2,
                                          rangeHighPercent = 1,
                                          rangeLowPercent = 0.01,
                                          excludedCovariateIds = NULL) {
  covariateData1 <-
    checkFilterTemporalCovariateDataByTimeIdCohortId(
      covariateData = covariateData1,
      cohortId = cohortId1,
      timeId = timeId1,
      multipleCohortId = multipleCohortId,
      excludedCovariateIds = excludedCovariateIds
    )
  covariateData1$covariates <- covariateData1$covariates |>
    dplyr::filter(averageValue <= rangeHighPercent,
                  averageValue >= rangeLowPercent)
  
  if (!is.null(covariateData2)) {
    covariateData2 <-
      checkFilterTemporalCovariateDataByTimeIdCohortId(
        covariateData = covariateData2,
        cohortId = cohortId2,
        timeId = timeId2,
        multipleCohortId = multipleCohortId,
        excludedCovariateIds = excludedCovariateIds
      )
    covariateData2$covariates <- covariateData2$covariates |>
      dplyr::filter(averageValue <= rangeHighPercent,
                    averageValue >= rangeLowPercent)
  }
  
  report <- FeatureExtraction::createTable1(
    covariateData1 = covariateData1,
    covariateData2 = covariateData2,
    cohortId1 = cohortId1,
    cohortId2 = cohortId2,
    specifications = table1Specifications,
    output = "one column",
    showCounts = showCounts,
    showPercent = showPercent,
    percentDigits = percentDigits,
    valueDigits = valueDigits,
    stdDiffDigits = stdDiffDigits
  )
  
  report <- report |> 
    dplyr::mutate(analysisName = dplyr::if_else(
      substr(Characteristic, 1, 2) != "  ",
      Characteristic,
      NA_character_
    )) |>
    tidyr::fill(analysisName, .direction = "down") |>
    dplyr::left_join(
      covariateData1$analysisRef |>
        dplyr::select(analysisId,
                      analysisName) |>
        dplyr::collect() |>
        dplyr::mutate(
          analysisName = SqlRender::camelCaseToTitleCase(analysisName) |> stringr::str_squish()
        ),
      by = "analysisName"
    )
  
  reportFields <- report |>
    dplyr::select(analysisId, Characteristic) |>
    dplyr::mutate(isHeader = ifelse(substring(Characteristic, 1, 1) != " ", 1, 0)) |>
    dplyr::left_join(
      table1Specifications |>
        dplyr::select(label,
                      analysisId) |>
        dplyr::rename(Characteristic = label),
      by = c("analysisId", "Characteristic")
    ) |>
    tidyr::fill(analysisId, .direction = "down") |>
    dplyr::left_join(
      covariateData1$covariateRef |>
        dplyr::collect() |>
        dplyr::mutate(Characteristic = paste0(
          "  ", stringr::str_remove(covariateName, ".*: ")
        )) |>
        dplyr::select(-covariateName,-covariateId),
      by = c("analysisId", "Characteristic")
    ) |>
    dplyr::mutate(Percent = stringr::str_squish(Characteristic)) |>
    dplyr::select(isHeader,
                  analysisId,
                  conceptId,
                  Characteristic) |> 
    dplyr::distinct()
  
  report <- reportFields |>
    dplyr::left_join(report,
                     by = c("analysisId", "Characteristic")) |> 
    dplyr::relocate(isHeader,
                    analysisId,
                    analysisName,
                    conceptId,
                    Characteristic)
  
  colnamesReport <- colnames(report)
  
  covariateData1Count <-
    attr(covariateData1, "metaData")$populationSize
  if (length(covariateData1Count) > 0) {
    covariateData1Count <-
      covariateData1Count[[which(names(covariateData1Count) == cohortId1)]]
  }
  
  covariateData2Count <-
    attr(covariateData2, "metaData")$populationSize
  if (length(covariateData2Count) > 0) {
    covariateData2Count <-
      covariateData2Count[[which(names(covariateData2Count) == cohortId2)]]
  }
  
  if (is.null(cohortId2)) {
    firstRow <- dplyr::tibble(
      isHeader = 1,
      analysisId = 0,
      analysisName = "",
      conceptId = 0,
      Characteristic = c("Cohort Id", "Population"),
      Value = c(cohortId1,
                covariateData1Count |>
                  as.character())
    )
  } else {
    firstRow <- dplyr::tibble(
      isHeader = 1,
      analysisId = 0,
      analysisName = "",
      conceptId = 0,
      Characteristic = c("Cohort Id", "Population"),
      cohort1 = c(cohortId1,
                  covariateData1Count |>
                    as.character()),
      cohort2 = c(cohortId2,
                  covariateData2Count |>
                    as.character()),
      stdDiff = ""
    )
  }

  colnames(report) <- colnames(firstRow)
  
  report <- dplyr::bind_rows(firstRow,
                             report)
  
  if (!is.null(cohortId2)) {
    report <- report |> 
      dplyr::mutate(stdDiff = as.numeric(stdDiff))
  }
  
  report <- report |> 
    dplyr::mutate(id = dplyr::row_number()) |> 
    dplyr::relocate(id)
  
  return(report)
}


#' @export
copyCovariateDataObjects <- function(covariateData) {
  tempfileLocation <- tempfile()
  unlink(x = tempfileLocation,
         recursive = TRUE,
         force = TRUE)
  dir.create(path = tempfileLocation,
             showWarnings = FALSE,
             recursive = TRUE)
  suppressMessages(
    FeatureExtraction::saveCovariateData(
      covariateData = covariateData,
      file =
        file.path(tempfileLocation,
                  "covariateData1")
    )
  )
  FeatureExtraction::loadCovariateData(file.path(tempfileLocation,
                                                 "covariateData1"))
}

#' @export
duplicateCovariateDataObjects <- function(covariateData) {
  tempfileLocation <- tempfile()
  unlink(x = tempfileLocation,
         recursive = TRUE,
         force = TRUE)
  dir.create(path = tempfileLocation,
             showWarnings = FALSE,
             recursive = TRUE)
  
  covariateDataArray <- c()
  suppressMessages(
    FeatureExtraction::saveCovariateData(
      covariateData = covariateData,
      file =
        file.path(tempfileLocation,
                  "covariateData1")
    )
  )
  covariateData1 <-
    FeatureExtraction::loadCovariateData(file.path(tempfileLocation,
                                                   "covariateData1"))
  
  suppressMessages(
    FeatureExtraction::saveCovariateData(
      covariateData = covariateData1,
      file =
        file.path(tempfileLocation,
                  "covariateData2")
    )
  )
  covariateDataArray$covariateData2 <-
    FeatureExtraction::loadCovariateData(file.path(tempfileLocation,
                                                   "covariateData2"))
  covariateDataArray$covariateData1 <-
    FeatureExtraction::loadCovariateData(file.path(tempfileLocation,
                                                   "covariateData1"))
  
  covariateDataArray$path <- tempfileLocation
  
  return(covariateDataArray)
}

#' @export
createTable1FromCovariateDataInParallel <- function(cdmSources,
                                                    covariateData1Path,
                                                    covariateData1CohortId,
                                                    covariateData2Path = NULL,
                                                    covariateData2CohortId = NULL,
                                                    analysisName = NULL,
                                                    analysisId = NULL,
                                                    timeId1 = NULL,
                                                    timeId2 = NULL,
                                                    table1Specifications = NULL,
                                                    showCounts = FALSE,
                                                    showPercent = TRUE,
                                                    percentDigits = 1,
                                                    valueDigits = 1,
                                                    stdDiffDigits = 2,
                                                    rangeHighPercent = 1,
                                                    rangeLowPercent = 0.01,
                                                    excludedCovariateIds = NULL,
                                                    label = "databaseId") {
  sourceKeys <- cdmSources$sourceKey |> unique()
  
  covariateData1Files <-
    list.files(
      path = covariateData1Path,
      pattern = "covariateData",
      all.files = TRUE,
      recursive = TRUE,
      ignore.case = TRUE,
      full.names = TRUE,
      include.dirs = TRUE
    )
  covariateData1Files <-
    covariateData1Files[stringr::str_detect(string = covariateData1Files,
                                            pattern = unique(sourceKeys) |> paste0(collapse = "|"))]
  
  covariateData1Files <-
    dplyr::tibble(filePath = covariateData1Files,
                  sourceKey = covariateData1Files |> dirname() |> basename())
  
  covariateData2Files <- NULL
  if (!is.null(covariateData2Path)) {
    covariateData2Files <-
      list.files(
        path = covariateData2Path,
        pattern = "covariateData",
        all.files = TRUE,
        recursive = TRUE,
        ignore.case = TRUE,
        full.names = TRUE,
        include.dirs = TRUE
      )
    covariateData2Files <-
      covariateData2Files[stringr::str_detect(string = covariateData2Files,
                                              pattern = unique(sourceKeys) |> paste0(collapse = "|"))]
    covariateData2Files <-
      dplyr::tibble(filePath = covariateData2Files,
                    sourceKey = covariateData2Files |> dirname() |> basename())
  }
  
  report <- c()
  counter <- 0
  for (i in (1:length(sourceKeys))) {
    sourceKey <- sourceKeys[[i]]
    covariateData1File <- covariateData1Files |>
      dplyr::filter(sourceKey == !!sourceKey)
    if (nrow(covariateData1File) == 1) {
      covariateData1 <-
        FeatureExtraction::loadCovariateData(file = covariateData1File$filePath)
      
      if (nrow(covariateData1$covariateRef |> dplyr::collect()) > 0) {
        covariateData1Temp <-
          copyCovariateDataObjects(covariateData = covariateData1)
        
        covariateData2Temp <- NULL
        if (!is.null(covariateData2Files)) {
          covariateData2File <- covariateData2Files |>
            dplyr::filter(sourceKey == !!sourceKey)
          
          if (nrow(covariateData2Files) == 1) {
            covariateData2 <-
              FeatureExtraction::loadCovariateData(file = covariateData2File$filePath)
            
            if (nrow(covariateData2$covariateRef |> dplyr::collect()) > 0) {
              covariateData2Temp <-
                copyCovariateDataObjects(covariateData = covariateData2)
            }
          }
        }
        counter <- counter + 1
        
        reportX <-
          createTable1FromCovariateData(
            covariateData1 = covariateData1Temp,
            covariateData2 = covariateData2Temp,
            cohortId1 = covariateData1CohortId,
            cohortId2 = covariateData2CohortId,
            analysisName = analysisName,
            analysisId = analysisId,
            timeId1 = timeId1,
            timeId2 = timeId2,
            table1Specifications = table1Specifications,
            showCounts = showCounts,
            showPercent = showPercent,
            percentDigits = percentDigits,
            valueDigits = valueDigits,
            stdDiffDigits = valueDigits,
            rangeHighPercent = rangeHighPercent,
            rangeLowPercent = rangeLowPercent,
            excludedCovariateIds = excludedCovariateIds
          )
        
        if (!is.null(reportX)) {
          if ("Percent" %in% colnames(reportX)) {
            reportX <- reportX |>
              dplyr::rename(!!sourceKey := Percent)
          }
          report[[counter]] <- reportX
        }
      }
    }
  }
  
  if (is.null(covariateData2Path)) {
    if (!is.null(report)) {
      commonColumns <- dplyr::bind_rows(report) |>
        dplyr::select(isHeader,
                      analysisId,
                      Characteristic) |>
        dplyr::distinct() |>
        dplyr::arrange(analysisId,
                       dplyr::desc(isHeader),
                       Characteristic)
      
      for (x in (1:length(report))) {
        if (!is.null(report[[x]])) {
          commonColumns <- commonColumns |>
            dplyr::left_join(report[[x]],
                             by = c("Characteristic",
                                    "isHeader",
                                    "analysisId"))
        }
      }
      report <- commonColumns
    }
  } else {
    browser()
  }
  
  if (!is.null(report)) {
    if (!is.null(label)) {
      if (label %in% colnames(cdmSources)) {
        dataSourcesLabel <- cdmSources |>
          dplyr::select("sourceKey", dplyr::all_of(label)) |>
          dplyr::distinct()
        colnames(dataSourcesLabel) <- c("originalName",
                                        "newName")
        
        reportColumnNames <-
          dplyr::tibble(originalName = colnames(report)) |>
          dplyr::left_join(dataSourcesLabel,
                           by = "originalName") |>
          dplyr::mutate(newName = dplyr::coalesce(newName, originalName))
        colnames(report) <- reportColumnNames$newName
      }
    }
    report <- report |>
      dplyr::arrange(analysisId,
                     dplyr::desc(isHeader),
                     dplyr::desc(4)) |>
      dplyr::mutate(dplyr::across(dplyr::everything(), ~ ifelse(is.na(.), "", .)))
    return(report)
  } else {
    writeLines("No output generated.")
  }
}



#' @export
createFeatureExtractionReportInParallel <- function(cdmSources,
                                                    covariateDataPath,
                                                    cohortId,
                                                    analysisName = NULL,
                                                    analysisId = NULL,
                                                    timeId = NULL,
                                                    rangeHighPercent = 1,
                                                    rangeLowPercent = 0.01,
                                                    excludedCovariateIds = NULL,
                                                    includedCovariateIds = NULL,
                                                    valueColumn = "averageValue",
                                                    pivotWider = TRUE,
                                                    pivotBy = "databaseId") {
  sourceKeys <- cdmSources$sourceKey |> unique()
  
  covariateDataFiles <-
    list.files(
      path = covariateDataPath,
      pattern = "covariateData",
      all.files = TRUE,
      recursive = TRUE,
      ignore.case = TRUE,
      full.names = TRUE,
      include.dirs = TRUE
    )
  covariateDataFiles <-
    covariateDataFiles[stringr::str_detect(string = covariateDataFiles,
                                           pattern = unique(sourceKeys) |> paste0(collapse = "|"))]
  
  covariateDataFiles <-
    dplyr::tibble(filePath = covariateDataFiles,
                  sourceKey = covariateDataFiles |> dirname() |> basename())
  
  
  report <- c()
  for (i in (1:length(sourceKeys))) {
    sourceKey <- sourceKeys[[i]]
    covariateDataFile <- covariateDataFiles |>
      dplyr::filter(sourceKey == !!sourceKey)
    if (nrow(covariateDataFile) == 1) {
      covariateData <-
        FeatureExtraction::loadCovariateData(file = covariateDataFile$filePath)
      
      if (nrow(covariateData$covariateRef |> dplyr::collect()) > 0) {
        covariateDataTemp <-
          copyCovariateDataObjects(covariateData = covariateData)
        
        covariateDataTemp <-
          checkFilterTemporalCovariateDataByTimeIdCohortId(
            covariateData = covariateDataTemp,
            cohortId = cohortId,
            timeId = timeId,
            multipleCohortId = multipleCohortId,
            excludedCovariateIds = excludedCovariateIds
          )
        
        if (!is.null(rangeHighPercent)) {
          covariateDataTemp$covariates <- covariateDataTemp$covariates |>
            dplyr::filter(averageValue <= rangeHighPercent)
        }
        
        if (!is.null(rangeLowPercent)) {
          covariateDataTemp$covariates <- covariateDataTemp$covariates |>
            dplyr::filter(averageValue >= rangeLowPercent)
        }
        
        report[[i]] <- covariateDataTemp$covariates |>
          dplyr::inner_join(covariateDataTemp$covariateRef,
                            by = "covariateId") |>
          dplyr::inner_join(covariateDataTemp$analysisRef,
                            by = "analysisId") |>
          dplyr::collect() |>
          dplyr::mutate(
            sourceKey = !!sourceKey,
            databaseId = cdmSources |>
              dplyr::filter(sourceKey == !!sourceKey) |>
              dplyr::pull(database)
          ) |>
          dplyr::mutate(
            countPercent = OhdsiHelpers::formatCountPercent(
              count = sumValue,
              percent = averageValue,
              percentDigits = 2
            )
          ) |>
          dplyr::mutate(averageValue = round(averageValue * 100, digits = 2)) |>
          dplyr::relocate(cohortDefinitionId,
                          covariateId,
                          conceptId,
                          covariateName)
        
        if (FeatureExtraction::isTemporalCovariateData(covariateDataTemp)) {
          report[[i]] <- report[[i]] |>
            dplyr::left_join(covariateDataTemp$timeRef |>
                               dplyr::collect(),
                             by = "timeId") |>
            dplyr::relocate(
              cohortDefinitionId,
              covariateId,
              conceptId,
              covariateName,
              timeId,
              startDay,
              endDay
            )
        }
      }
    }
  }
  
  report <- dplyr::bind_rows(report)
  
  if (pivotWider) {
    report <- report |>
      tidyr::pivot_wider(
        id_cols = setdiff(
          colnames(report),
          c(
            "sumValue",
            "averageValue",
            "databaseId",
            "countPercent",
            "sourceKey"
          )
        ),
        names_from = !!pivotBy,
        values_from = !!valueColumn,
        values_fill = 0
      )
  }
  return(report)
}