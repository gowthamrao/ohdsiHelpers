#' @export
getFeatureExtractionReportByTimeWindows <-
  function(covariateData,
           startDays = NULL,
           endDays = NULL,
           includeNonTimeVarying = FALSE,
           minAverageValue = 0.01,
           includedCovariateIds = NULL,
           excludedCovariateIds = NULL,
           table1specifications = NULL,
           cohortId,
           cohortDefinitionSet,
           databaseId = NULL,
           cohortName = NULL,
           reportName = NULL,
           format = TRUE,
           distributionStatistic = c("averageValue",
                                     "standardDeviation",
                                     "medianValue",
                                     "p25Value",
                                     "p75Value"),
           pivot = TRUE) {
    if (!is.null(table1specifications)) {
      if (nrow(table1specifications) == 0) {
        stop("please check table1specifications")
      }
      includedCovariateIdsFromTable1Specifications <-
        table1specifications$covariateIds |> paste(collapse = ",") |> commaSeparaedStringToIntArray()
      
      if (!is.null(includedCovariateIds)) {
        includedCovariateIds <- intersect(includedCovariateIds,
                                          includedCovariateIdsFromTable1Specifications)
      } else {
        includedCovariateIds <- includedCovariateIdsFromTable1Specifications
      }
    }
    
    covariateAnalysisId <- covariateData$covariateRef |>
      dplyr::select(covariateId,
                    analysisId,
                    covariateName,
                    conceptId) |>
      dplyr::inner_join(
        covariateData$analysisRef |>
          dplyr::select(analysisId,
                        analysisName,
                        domainId),
        by = "analysisId"
      )
    
    if (!is.null(includedCovariateIds)) {
      covariateAnalysisId <- covariateAnalysisId |>
        dplyr::filter(covariateId %in% includedCovariateIds)
    }
    
    if (!is.null(excludedCovariateIds)) {
      covariateAnalysisId <- covariateAnalysisId |>
        dplyr::filter(!covariateId %in% excludedCovariateIds)
    }
    
    reportTimeVarying <- dplyr::tibble()
    reportNonTimeVarying <- dplyr::tibble()
    
    if (all(is.null(startDays),
            is.null(endDays))) {
      if (covariateData$covariates |>
          dplyr::select(covariateId) |>
          dplyr::distinct() |>
          dplyr::inner_join(covariateAnalysisId |>
                            dplyr::select(covariateId) |>
                            dplyr::distinct()) |>
          dplyr::collect() |>
          nrow() > 1) {
        message(
          "startDays and endDays is NULL, but covariateId is time varying. Report may be incomplete"
        )
      }
    }
    
    if (any(!is.null(startDays),!is.null(endDays))) {
      reportTimeVarying1 <- covariateData$covariates |>
        dplyr::filter(cohortDefinitionId == cohortId) |>
        dplyr::inner_join(
          covariateData$timeRef |>
            dplyr::filter(startDay %in% startDays,
                          endDay %in% endDays) |>
            dplyr::mutate(
              periodName = paste0("d",
                                  startDay |> as.integer(),
                                  "d",
                                  endDay |> as.integer())
            ),
          by = "timeId"
        ) |>
        dplyr::inner_join(covariateAnalysisId,
                          by = "covariateId") |>
        dplyr::arrange(startDay,
                       endDay,
                       dplyr::desc(averageValue)) |>
        dplyr::select(
          covariateId,
          covariateName,
          conceptId,
          analysisId,
          analysisName,
          domainId,
          periodName,
          sumValue,
          averageValue
        ) |>
        dplyr::filter(averageValue > minAverageValue) |>
        dplyr::collect() |>
        dplyr::mutate(continuous = 0)
      
      reportTimeVarying2a <-
        covariateData$covariatesContinuous |>
        dplyr::filter(cohortDefinitionId == cohortId) |>
        dplyr::inner_join(
          covariateData$timeRef |>
            dplyr::filter(startDay %in% startDays,
                          endDay %in% endDays) |>
            dplyr::mutate(
              periodName = paste0("d",
                                  startDay |> as.integer(),
                                  "d",
                                  endDay |> as.integer())
            ),
          by = "timeId"
        ) |>
        dplyr::inner_join(covariateAnalysisId,
                          by = "covariateId") |>
        dplyr::arrange(startDay,
                       endDay,
                       dplyr::desc(averageValue)) |>
        dplyr::select(
          "covariateId",
          "covariateName",
          "conceptId",
          "analysisId",
          "analysisName",
          "domainId",
          "periodName",
          distributionStatistic
        ) |>
        dplyr::filter(averageValue > minAverageValue) |>
        dplyr::collect() |>
        tidyr::pivot_longer(
          cols = c(distributionStatistic),
          names_to = "statistic",
          values_to = "averageValue"
        ) |>
        dplyr::mutate(statistic = stringr::str_replace(
          string = statistic,
          pattern = "Value",
          replacement = ""
        )) |>
        dplyr::mutate(covariateName = paste0(covariateName, " (", statistic, ")")) |>
        dplyr::select(-statistic)
      
      reportTimeVarying2b <-
        covariateData$covariatesContinuous |>
        dplyr::filter(cohortDefinitionId == cohortId)  |>
        dplyr::inner_join(
          covariateData$timeRef |>
            dplyr::filter(startDay %in% startDays,
                          endDay %in% endDays) |>
            dplyr::mutate(
              periodName = paste0("d",
                                  startDay |> as.integer(),
                                  "d",
                                  endDay |> as.integer())
            ),
          by = "timeId"
        ) |>
        dplyr::inner_join(covariateAnalysisId,
                          by = "covariateId") |>
        dplyr::select(covariateId,
                      periodName,
                      countValue) |>
        dplyr::rename(sumValue = countValue) |>
        dplyr::collect()
      
      reportTimeVarying2 <- reportTimeVarying2a |>
        dplyr::inner_join(reportTimeVarying2b,
                          by = c("covariateId",
                                 "periodName")) |>
        dplyr::mutate(continuous = 1)
      
      reportTimeVarying <- dplyr::tibble()
      if (nrow(reportTimeVarying1) > 0) {
        reportTimeVarying <- dplyr::bind_rows(reportTimeVarying,
                                              reportTimeVarying1)
      }
      if (nrow(reportTimeVarying2) > 0) {
        reportTimeVarying <- dplyr::bind_rows(reportTimeVarying,
                                              reportTimeVarying2)
      }
      
      if (all(nrow(reportTimeVarying) > 0,
              length(colnames(reportTimeVarying) > 0),
              format)) {
        reportTimeVarying <-
          dplyr::bind_rows(
            reportTimeVarying |>
              dplyr::filter(continuous == 0) |>
              dplyr::mutate(
                report = OhdsiHelpers::formatCountPercent(count = sumValue, percent = averageValue)
              ),
            reportTimeVarying |>
              dplyr::filter(continuous == 1) |>
              dplyr::mutate(
                report = OhdsiHelpers::formatDecimalWithComma(
                  number = averageValue,
                  decimalPlaces = 1,
                  round = 1
                )
              )
          ) |>
          dplyr::select(-continuous)
      } else {
        reportTimeVarying <- dplyr::tibble()
      }
    }
    
    if (includeNonTimeVarying) {
      if ("timeId" %in% colnames(covariateData$covariates)) {
        covariateData <-  covariateData$covariates |>
          dplyr::filter(cohortDefinitionId == cohortId) |>
          dplyr::filter(is.na(timeId))
      } else {
        covariateData <-  covariateData$covariates |>
          dplyr::filter(cohortDefinitionId == cohortId)
      }
      
      reportNonTimeVarying1 <- covariateData |>
        dplyr::inner_join(covariateAnalysisId,
                          by = "covariateId") |>
        dplyr::arrange(dplyr::desc(averageValue)) |>
        dplyr::mutate(periodName = "nonTimeVarying") |>
        dplyr::select(
          covariateId,
          covariateName,
          conceptId,
          analysisId,
          analysisName,
          domainId,
          periodName,
          sumValue,
          averageValue
        ) |>
        dplyr::filter(averageValue > minAverageValue) |>
        dplyr::collect() |>
        dplyr::mutate(continuous = 0)
      
      if ("timeId" %in% colnames(covariateData$covariatesContinuous)) {
        covariatesContinuous <-  covariateData$covariatesContinuous |>
          dplyr::filter(cohortDefinitionId == cohortId) |>
          dplyr::filter(is.na(timeId))
      } else {
        covariatesContinuous <-  covariateData$covariatesContinuous |>
          dplyr::filter(cohortDefinitionId == cohortId)
      }
      
      reportNonTimeVarying2a <-
        covariatesContinuous |>
        dplyr::inner_join(covariateAnalysisId,
                          by = "covariateId") |>
        dplyr::arrange(dplyr::desc(averageValue)) |>
        dplyr::mutate(periodName = "nonTimeVarying") |>
        dplyr::select(
          "covariateId",
          "covariateName",
          "conceptId",
          "analysisId",
          "analysisName",
          "domainId",
          "periodName",
          distributionStatistic
        ) |>
        dplyr::filter(averageValue > minAverageValue) |>
        dplyr::collect() |>
        tidyr::pivot_longer(
          cols = c(distributionStatistic),
          names_to = "statistic",
          values_to = "averageValue"
        ) |>
        dplyr::mutate(statistic = stringr::str_replace(
          string = statistic,
          pattern = "Value",
          replacement = ""
        )) |>
        dplyr::mutate(covariateName = paste0(covariateName, " (", statistic, ")")) |>
        dplyr::select(-statistic)
      
      reportNonTimeVarying2b <-
        covariatesContinuous |>
        dplyr::inner_join(covariateAnalysisId,
                          by = "covariateId") |>
        dplyr::arrange(dplyr::desc(averageValue)) |>
        dplyr::mutate(periodName = "nonTimeVarying") |>
        dplyr::select(covariateId,
                      periodName,
                      countValue) |>
        dplyr::rename(sumValue = countValue) |>
        dplyr::collect()
      
      reportNonTimeVarying2 <- reportNonTimeVarying2a |>
        dplyr::inner_join(reportNonTimeVarying2b,
                          by = c("covariateId",
                                 "periodName")) |>
        dplyr::mutate(continuous = 1)
      
      reportNonTimeVarying <- dplyr::tibble()
      if (nrow(reportNonTimeVarying1) > 0) {
        reportNonTimeVarying <- dplyr::bind_rows(reportNonTimeVarying,
                                                 reportNonTimeVarying1)
      }
      if (nrow(reportNonTimeVarying2) > 0) {
        reportNonTimeVarying <- dplyr::bind_rows(reportNonTimeVarying,
                                                 reportNonTimeVarying2)
      }
      
      if (all(nrow(reportNonTimeVarying) > 0,
              length(colnames(reportNonTimeVarying) > 0),
              format)) {
        reportNonTimeVarying <-
          dplyr::bind_rows(
            reportNonTimeVarying |>
              dplyr::filter(continuous == 0) |>
              dplyr::mutate(
                report = OhdsiHelpers::formatCountPercent(count = sumValue, percent = averageValue)
              ),
            reportNonTimeVarying |>
              dplyr::filter(continuous == 1) |>
              dplyr::mutate(
                report = OhdsiHelpers::formatDecimalWithComma(
                  number = averageValue,
                  decimalPlaces = 1,
                  round = 1
                )
              )
          ) |>
          dplyr::select(-continuous)
      } else {
        reportNonTimeVarying <- dplyr::tibble()
      }
    }
    
    report <- dplyr::bind_rows(reportNonTimeVarying,
                               reportTimeVarying)
    
    if (all(nrow(report) > 0,
            'conceptId' %in% colnames(report))) {
      report <- report |>
        dplyr::mutate(conceptId = dplyr::if_else(
          condition = (conceptId == 0),
          true = (covariateId - analysisId) / 1000,
          false = conceptId
        ))
    }
    
    rm("covariateAnalysisId")
    
    if (nrow(report) == 0) {
      writeLines("No results")
      return()
    }
    
    if (!is.null(table1specifications)) {
      table1specifications <- table1specifications |>
        dplyr::mutate(labelId = dplyr::row_number()) |>
        dplyr::relocate(labelId)
      reportTable1 <- c()
      for (i in (1:nrow(table1specifications))) {
        reportTable1[[i]] <- table1specifications[i,] |>
          dplyr::select(labelId,
                        label) |>
          tidyr::crossing(report |>
                            dplyr::filter(
                              covariateId %in% commaSeparaedStringToIntArray(table1specifications[i,]$covariateIds)
                            ))
        
        if (nrow(reportTable1[[i]]) > 0) {
          reportTable1[[i]] <- dplyr::bind_rows(
            reportTable1[[i]] |>
              dplyr::mutate(source = 2),
            table1specifications[i,] |>
              dplyr::mutate(
                covariateId = 0,
                periodName = "",
                covariateName = label
              ) |>
              dplyr::select(labelId,
                            label,
                            covariateId,
                            periodName,
                            covariateName) |>
              dplyr::mutate(source = 1)
          ) |>
            dplyr::arrange(labelId,
                           label,
                           source) |>
            dplyr::select(-source)
        }
      }
      report <- dplyr::bind_rows(reportTable1)
      
      idCols <- c(
        "labelId",
        "label",
        "covariateId",
        "covariateName",
        "conceptId",
        "analysisId",
        "analysisName",
        "domainId"
      )
    } else {
      idCols <- c(
        "covariateId",
        "covariateName",
        "conceptId",
        "analysisId",
        "analysisName",
        "domainId"
      )
    }
    
    if (pivot) {
      report <- report |>
        tidyr::pivot_wider(id_cols = idCols,
                           names_from = periodName,
                           values_from = report)
    }
    
    if (!is.null(cohortName)) {
      report <-
        dplyr::bind_rows(report |>
                           dplyr::slice(0),
                         dplyr::tibble(covariateName = cohortName),
                         report)
    }
    
    if (!is.null(databaseId)) {
      report <- dplyr::bind_rows(report |>
                                   dplyr::slice(0),
                                 dplyr::tibble(covariateName = databaseId),
                                 report)
    }
    
    if (!is.null(cohortName)) {
      report <-
        dplyr::bind_rows(report |>
                           dplyr::slice(0),
                         dplyr::tibble(covariateName = cohortName),
                         report)
    }
    
    if (!is.null(reportName)) {
      report <-
        dplyr::bind_rows(report |>
                           dplyr::slice(0),
                         dplyr::tibble(covariateName = reportName),
                         report)
    }
    
    return(report)
  }



#' @export
getFeatureExtractionReportInParallel <-
  function(cdmSources,
           covariateDataPath,
           startDays = NULL,
           endDays = NULL,
           includeNonTimeVarying = FALSE,
           minAverageValue = 0.01,
           includedCovariateIds = NULL,
           excludedCovariateIds = NULL,
           table1specifications = NULL,
           simple = TRUE,
           cohortId,
           covariateDataFileNamePattern =  paste0(cohortId, "$"),
           cohortDefinitionSet,
           databaseId = NULL,
           cohortName = NULL,
           reportName = NULL,
           format = TRUE) {
    sourceKeys <- cdmSources$sourceKey |> unique()
    
    covariateDataFiles <-
      list.files(
        path = covariateDataPath,
        pattern = paste0("^", cohortId, "$"),
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
        reportFe <-
          getFeatureExtractionReportByTimeWindows(
            covariateData = covariateData,
            startDays = startDays,
            endDays = endDays,
            includeNonTimeVarying = includeNonTimeVarying,
            minAverageValue = minAverageValue,
            includedCovariateIds = includedCovariateIds,
            excludedCovariateIds = excludedCovariateIds,
            table1specifications = table1specifications,
            cohortId = cohortId,
            cohortDefinitionSet = cohortDefinitionSet,
            databaseId = databaseId,
            cohortName = cohortName,
            reportName = reportName,
            format = format,
            pivot = FALSE
          )
        
        if (!is.null(reportFe)) {
          if (nrow(reportFe) > 0) {
            databaseId <- cdmSources |>
              dplyr::filter(sourceKey == !!sourceKey) |>
              dplyr::pull(database)
            report[[i]] <- reportFe |>
              dplyr::mutate(databaseId = databaseId)
          }
        }
      }
    }
    
    if (is.null(report)) {
      return(NULL)
    }
    
    report <- dplyr::bind_rows(report) |>
      dplyr::mutate(Characteristic = paste0("  ", stringr::str_remove(covariateName, ".*: ")))
    
    idCols <- setdiff(colnames(report),
                      c("sumValue",
                        "averageValue",
                        "report",
                        "databaseId"))
    
    if (simple) {
      idCols <- intersect(idCols,
                          c(
                            "labelId",
                            "label",
                            "covariateId",
                            "Characteristic",
                            "periodName"
                          ))
    }
    
    report <- report |>
      tidyr::pivot_wider(
        id_cols = idCols,
        names_from = "databaseId",
        values_from = "report",
        values_fill = "0"
      )
    
    if (!is.null(table1specifications)) {
      report <- report |>
        dplyr::arrange(labelId,
                       label,
                       periodName,
                       covariateId,
                       Characteristic) |>
        dplyr::filter(!is.na(labelId))
    } else {
      report <- report |>
        dplyr::arrange(dplyr::desc(setdiff(colnames(report),
                                           idCols)[[1]])) |>
        dplyr::filter(!is.na(covariateId))
    }
    
    return(report)
  }



#' @export
getFeatureExtractionReportCommonSequentialTimePeriods <-
  function() {
    # covariateData$timeRef |>
    #   dplyr::collect() |>
    #   dplyr::filter(endDay == -1) |>
    #   dplyr::filter(startDay %% 2 != 0) |>
    #   dplyr::filter(startDay > -400) |>
    #   dplyr::filter(startDay != -365)
    #
    # covariateData$timeRef |>
    #   dplyr::collect() |>
    #   dplyr::filter(startDay == 1) |>
    #   dplyr::filter(endDay %% 2 != 0) |>
    #   dplyr::filter(endDay != 365) |>
    #   dplyr::filter(endDay < 400)
    
    
    priorMonthlyPeriods <- dplyr::tibble(
      timeId = c(15, 21, 23, 25, 27, 29, 31, 33, 36, 38, 41, 44, 47),
      startDay = c(
        -391,-361,-331,-301,-271,-241,-211,-181,-151,-121,-91,-61,-31
      ),
      endDay = c(-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1)
    )
    
    postMonthlyPeriods <- dplyr::tibble(
      timeId = c(58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70),
      startDay = c(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1),
      endDay = c(1, 31, 61, 91, 121, 151, 181, 211, 241, 271, 301, 331, 361)
    )
    
    onDayOf <- dplyr::tibble(timeId = 53,
                             startDay = 0,
                             endDay = 0)
    
    timePeriods <- dplyr::bind_rows(priorMonthlyPeriods,
                                    postMonthlyPeriods,
                                    onDayOf) |>
      dplyr::arrange(timeId)
    
    return(timePeriods)
    
  }



#' @export
getFeatureExtractionReportNonTimeVarying <-
  function(cdmSources,
           covariateDataPath,
           cohortId,
           cohortDefinitionSet) {
    
    output <- c()
    output$rawData <-
      getFeatureExtractionReportInParallel(
        cdmSources = cdmSources,
        covariateDataPath = covariateDataPath,
        includeNonTimeVarying = TRUE,
        minAverageValue = 0.01,
        includedCovariateIds = NULL,
        excludedCovariateIds = NULL,
        table1specifications = NULL,
        simple = TRUE,
        cohortId,
        covariateDataFileNamePattern =  paste0(cohortId, "$"),
        cohortDefinitionSet,
        databaseId = NULL,
        cohortName = NULL,
        reportName = NULL,
        format = TRUE
      )
    
    browser()
    
    return(output)
  }
