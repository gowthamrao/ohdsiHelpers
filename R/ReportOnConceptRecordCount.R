#' @export
reportOnConceptRecordCount <-
  function(conceptRecordCount,
           conceptSetId,
           conceptSetDefinition,
           cdmSources,
           conceptIdDetails,
           domainTable = "drug_exposure",
           domainField = "drug_concept_id",
           incidence = 1,
           caption = "Drug utilization") {
    if (nrow(conceptSetDefinition) > 1) {
      stop("more than one concept set definition")
    }
    
    sourceKey <- cdmSources |>
      dplyr::select(sourceKey,
                    databaseShortName) |>
      dplyr::rename(database = databaseShortName) |>
      dplyr::mutate(
        database = database |>
          SqlRender::snakeCaseToCamelCase() |>
          SqlRender::camelCaseToTitleCase()
      )
    
    # table 1: concept set by data source ----
    conceptSetId <-
      conceptSetDefinition$conceptSetId |> as.numeric()
    personsWithConceptSet <-
      OhdsiHelpers::filterDataFrame(
        data = conceptRecordCount,
        conceptId == 0,
        conceptSetId == !!conceptSetId,
        isSourceField == 0,
        genderConceptId == 0,
        calendarYear == 0,
        calendarQuarter == 0,
        calendarMonth == 0,
        incidence == !!incidence,
        ageGroup == -1,
        domainTable == !!domainTable,
        domainField == !!domainField
      )
    
    malesWithExposure <-
      OhdsiHelpers::filterDataFrame(
        data = conceptRecordCount,
        conceptId == 0,
        conceptSetId == !!conceptSetId,
        isSourceField == 0,
        genderConceptId == 8507,
        calendarYear == 0,
        calendarQuarter == 0,
        calendarMonth == 0,
        incidence == !!incidence,
        ageGroup == -1,
        domainTable == !!domainTable,
        domainField == !!domainField
      )
    
    conceptSetByDataSource <- personsWithConceptSet |>
      dplyr::select(sourceKey,
                    subjectCount,
                    minDate,
                    maxDate,
                    ageAvg) |>
      dplyr::left_join(
        malesWithExposure |>
          dplyr::select(sourceKey,
                        subjectCount) |>
          dplyr::rename(maleCount = subjectCount)
      ) |>
      dplyr::mutate(male = maleCount / subjectCount) |>
      dplyr::inner_join(sourceKey) |>
      dplyr::select(database,
                    subjectCount,
                    ageAvg,
                    male,
                    minDate,
                    maxDate) |>
      dplyr::arrange(dplyr::desc(subjectCount))
    
    # table 2: concepts by data source ----
    standardConceptsByDataSource <-
      conceptIdDetails |>
      dplyr::inner_join(
        OhdsiHelpers::filterDataFrame(
          data = conceptRecordCount,
          conceptSetId == !!conceptSetId,
          conceptId > 0,
          isSourceField == 0,
          genderConceptId == 0,
          calendarYear == 0,
          calendarQuarter == 0,
          calendarMonth == 0,
          incidence == !!incidence,
          ageGroup == -1,
          domainTable == "drug_exposure",
          domainField == "drug_concept_id"
        )
      ) |>
      dplyr::select(conceptName,
                    conceptClassId,
                    sourceKey,
                    subjectCount) |>
      dplyr::inner_join(sourceKey) |>
      dplyr::select(database,
                    conceptName,
                    conceptClassId,
                    subjectCount) |>
      dplyr::group_by(database,
                      conceptName,
                      conceptClassId) |>
      dplyr::summarise(subjectCount = max(subjectCount)) |>
      dplyr::ungroup() |>
      tidyr::pivot_wider(
        id_cols = c(conceptName,
                    conceptClassId),
        names_from = database,
        values_fill = 0,
        values_from = subjectCount
      )
    
    # plot 3: concept set temporal trend ----
    
    conceptSetTrendByMonth <-
      OhdsiHelpers::filterDataFrame(
        data = conceptRecordCount,
        conceptId == 0,
        conceptSetId == !!conceptSetId,
        isSourceField == 0,
        genderConceptId == 0,
        calendarYear > 0,
        calendarQuarter > 0,
        calendarMonth > 0,
        incidence == 1,
        ageGroup == -1,
        domainTable == "drug_exposure",
        domainField == "drug_concept_id"
      )
    
    conceptSetTrendByMonth <- conceptSetTrendByMonth |>
      dplyr::mutate(date = as.Date(paste0(calendarYear,
                                          "-",
                                          calendarMonth,
                                          "-",
                                          1)),
                    count = subjectCount) |>
      dplyr::select(date,
                    count,
                    database) |>
      dplyr::arrange(date) |>
      OhdsiPlots::renderTrendGraph(groupBy = "database",
                                   smoothLines = TRUE,
                                   showRawValues = TRUE)
    
    
    # plot 4: concept set temporal trend ----
    
    # conceptSetTrendByQuarter <-
    #   OhdsiHelpers::filterDataFrame(
    #     data = conceptRecordCount,
    #     conceptId == 0,
    #     conceptSetId == !!conceptSetId,
    #     isSourceField == 0,
    #     genderConceptId == 0,
    #     calendarYear > 0,
    #     calendarQuarter > 0,
    #     calendarMonth == -1,
    #     incidence == 1,
    #     ageGroup == -1,
    #     domainTable == "drug_exposure",
    #     domainField == "drug_concept_id"
    #   )
    #
    # conceptSetTrendByQuarter <- conceptSetTrendByQuarter |>
    #   dplyr::mutate(date = as.Date(paste0(
    #     calendarYear,
    #     "-",
    #     calendarQuarter,
    #     "-",
    #     1
    #   )),
    #   count = subjectCount) |>
    #   dplyr::select(date,
    #                 count,
    #                 database) |>
    #   dplyr::arrange(date) |>
    #   OhdsiPlots::renderTrendGraph(groupBy = "database",
    #                                smoothLines = TRUE,
    #                                showRawValues = TRUE)
    
    report <- c()
    
    report$table1ConceptSetData <- conceptSetByDataSource
    report$table1ConceptSetPretty <- conceptSetByDataSource |>
      OhdsiHelpers::prettyReportKabbleTable(
        caption = caption,
        align = "l",
        formatAsInteger = c("subjectCount"),
        formatAsDecimals = c("ageAvg"),
        formatAsPercent = c("male")
      )
    report$table2standardConceptsByDataSource <-
      standardConceptsByDataSource
    report$table2StandardConceptsByDataSourcePretty <-
      standardConceptsByDataSource |>
      dplyr::arrange(dplyr::desc(standardConceptsByDataSource[[3]])) |>
      OhdsiHelpers::prettyReportKabbleTable(align = "l",
                                            formatAsInteger = setdiff(
                                              colnames(standardConceptsByDataSource),
                                              c("conceptName",
                                                "conceptClassId")
                                            ))
    report$plot3ConceptSetTrendByCalendarMonth <-
      conceptSetTrendByMonth
    # report$plot4ConceptSetTrendByCalendarQuarter <-
    #   conceptSetTrendByQuarter
    
    report$populationPyramidDataEntirePeriodData <-
      OhdsiHelpers::filterDataFrame(
        data = conceptRecordCount,
        conceptId == 0,
        conceptSetId == !!conceptSetId,
        isSourceField == 0,
        genderConceptId > 0,
        calendarQuarter == 0,
        calendarMonth == 0,
        calendarYear == 0,
        # calendarQUarter is not accurate at this time
        incidence == 1,
        ageGroup > 0,
        domainTable == "drug_exposure",
        domainField == "drug_concept_id"
      ) |>
      dplyr::filter(genderConceptId %in% c(8507, 8532)) |>
      dplyr::select(ageGroup,
                    genderConceptId,
                    calendarYear,
                    subjectCount,
                    sourceKey) |>
      dplyr::mutate(
        ageGroup = paste0(as.character(ageGroup * 10),
                          "-",
                          as.character(((
                            ageGroup + 1
                          ) * 10) - 1)),
        sex = dplyr::if_else(
          condition = (genderConceptId == 8507),
          true = "Male",
          false = "Female"
        ),
        count = subjectCount
      ) |>
      dplyr::inner_join(sourceKey) |>
      dplyr::select(database,
                    ageGroup,
                    sex,
                    count)
    
    dataSources <-
      report$populationPyramidDataEntirePeriodData$database |> unique() |> sort()
    
    populationPyramidDataEntirePeriodDataPlots <- c()
    for (j in (1:length(dataSources))) {
      dataSource <- dataSources[[j]]
      data <- report$populationPyramidDataEntirePeriodData |>
        dplyr::filter(database == dataSource)
      populationPyramidDataEntirePeriodDataPlots[[dataSource]] <-
        OhdsiPlots::createPopulationPyramid(data = data, title = dataSource)
    }
    report$populationPyramidDataEntirePeriodDataPlots <-
      populationPyramidDataEntirePeriodDataPlots
    
    report$populationPyramidDataEntirePeriodDataPlotsTrellis <-
      OhdsiPlots::createTrellis(ggplotArray = populationPyramidDataEntirePeriodDataPlots,
                                maxColumns = 3)
    
    return(report)
  }