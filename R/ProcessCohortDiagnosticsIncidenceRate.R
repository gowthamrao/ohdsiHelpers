#' @export
processCohortDiagnosticsIncidenceRate <-
  function(cohortDiagnosticsIncidenceRate,
           cohortId,
           cohortName,
           maxCalendarDate = NULL,
           minCalendarDate = NULL,
           gender = c('MALE', 'FEMALE'),
           ageGroup = NULL) {
    cohortDiagnosticsIncidenceRateFiltered <-
      cohortDiagnosticsIncidenceRate |>
      dplyr::filter(cohortId == !!cohortId) |>
      dplyr::mutate(ageGroupLabel = paste0(as.character(sprintf(
        "%02d", ageGroup *
          10
      )), " - ", as.character(sprintf(
        "%02d",
        (ageGroup *
           10) + 10
      ))))
    
    if (!is.null(maxCalendarDate)) {
      cohortDiagnosticsIncidenceRateFiltered <- cohortDiagnosticsIncidenceRateFiltered |> 
        dplyr::filter(calendarYear <= maxCalendarDate)
    }
    
    if (!is.null(minCalendarDate)) {
      cohortDiagnosticsIncidenceRateFiltered <- cohortDiagnosticsIncidenceRateFiltered |> 
        dplyr::filter(calendarYear >= minCalendarDate)
    }
    
    if (!is.null(gender)) {
      cohortDiagnosticsIncidenceRateFiltered <- cohortDiagnosticsIncidenceRateFiltered |> 
        dplyr::filter(gender %in% !!gender)
    }
    
    if (!is.null(ageGroup)) {
      cohortDiagnosticsIncidenceRateFiltered <- cohortDiagnosticsIncidenceRateFiltered |> 
        dplyr::filter(ageGroup %in% !!ageGroup)
    }
    
    incidenceRate <- c()
    
    incidenceRate$byCalendarYear <-
      cohortDiagnosticsIncidenceRateFiltered |>
      dplyr::group_by(databaseId,
                      calendarYear) |>
      dplyr::summarise(cohortCount = sum(cohortCount),
                       personYears = sum(personYears)) |>
      dplyr::mutate(ir = (cohortCount / personYears) * 1000) |>
      dplyr::mutate(
        cohortCountFormatted = OhdsiHelpers::formatIntegerWithComma(cohortCount),
        irFormatted = OhdsiHelpers::formatDecimalWithComma(ir, decimalPlaces = 3)
      )
    
    incidenceRate$byCalendarYearAgeGroup <-
      cohortDiagnosticsIncidenceRateFiltered |>
      dplyr::group_by(databaseId,
                      calendarYear,
                      ageGroup) |>
      dplyr::summarise(cohortCount = sum(cohortCount),
                       personYears = sum(personYears)) |>
      dplyr::mutate(ir = (cohortCount / personYears) * 1000) |>
      dplyr::mutate(
        cohortCountFormatted = OhdsiHelpers::formatIntegerWithComma(cohortCount),
        irFormatted = OhdsiHelpers::formatDecimalWithComma(ir, decimalPlaces = 3)
      ) |>
      dplyr::arrange(ageGroup)
    
    incidenceRate$byCalendarYearAgeGroupGender <-
      cohortDiagnosticsIncidenceRateFiltered |>
      dplyr::group_by(databaseId,
                      calendarYear,
                      ageGroup,
                      ageGroupLabel,
                      gender) |>
      dplyr::summarise(cohortCount = sum(cohortCount),
                       personYears = sum(personYears)) |>
      dplyr::mutate(ir = (cohortCount / personYears) * 1000) |>
      dplyr::mutate(
        cohortCountFormatted = OhdsiHelpers::formatIntegerWithComma(cohortCount),
        irFormatted = OhdsiHelpers::formatDecimalWithComma(ir, decimalPlaces = 3)
      ) |>
      dplyr::arrange(ageGroup)
    
    incidenceRate$overall <-
      cohortDiagnosticsIncidenceRateFiltered |>
      dplyr::group_by(databaseId) |>
      dplyr::summarise(cohortCount = sum(cohortCount),
                       personYears = sum(personYears)) |>
      dplyr::mutate(ir = (cohortCount / personYears) * 1000) |>
      dplyr::mutate(
        cohortCountFormatted = OhdsiHelpers::formatIntegerWithComma(cohortCount),
        irFormatted = OhdsiHelpers::formatDecimalWithComma(ir, decimalPlaces = 3)
      )
    
    incidenceRate$gender <-
      cohortDiagnosticsIncidenceRateFiltered |>
      dplyr::group_by(databaseId,
                      gender) |>
      dplyr::summarise(cohortCount = sum(cohortCount),
                       personYears = sum(personYears)) |>
      dplyr::mutate(ir = (cohortCount / personYears) * 1000) |>
      dplyr::mutate(
        cohortCountFormatted = OhdsiHelpers::formatIntegerWithComma(cohortCount),
        irFormatted = OhdsiHelpers::formatDecimalWithComma(ir, decimalPlaces = 3)
      )
    
    incidenceRate$age <- cohortDiagnosticsIncidenceRateFiltered |>
      dplyr::group_by(databaseId,
                      ageGroup) |>
      dplyr::summarise(cohortCount = sum(cohortCount),
                       personYears = sum(personYears)) |>
      dplyr::mutate(ir = (cohortCount / personYears) * 1000) |>
      dplyr::mutate(
        cohortCountFormatted = OhdsiHelpers::formatIntegerWithComma(cohortCount),
        irFormatted = OhdsiHelpers::formatDecimalWithComma(ir, decimalPlaces = 3)
      ) |>
      dplyr::arrange(ageGroup)
    
    incidenceRate$ageGender <-
      cohortDiagnosticsIncidenceRateFiltered |>
      dplyr::group_by(databaseId,
                      ageGroup,
                      gender) |>
      dplyr::summarise(cohortCount = sum(cohortCount),
                       personYears = sum(personYears)) |>
      dplyr::mutate(ir = (cohortCount / personYears) * 1000) |>
      dplyr::mutate(
        cohortCountFormatted = OhdsiHelpers::formatIntegerWithComma(cohortCount),
        irFormatted = OhdsiHelpers::formatDecimalWithComma(ir, decimalPlaces = 3)
      ) |>
      dplyr::arrange(ageGroup)
    
    incidenceRate$plot <-
      ggplot2::ggplot(
        incidenceRate$byCalendarYearAgeGroupGender,
        ggplot2::aes(
          x = calendarYear,
          y = ir,
          group = gender,
          color = gender
        )
      ) +
      ggplot2::geom_line() +
      ggplot2::scale_color_manual(values = c("MALE" = "blue", "FEMALE" = "red")) +
      ggplot2::facet_grid(databaseId ~ ageGroupLabel) +
      ggplot2::labs(title = cohortName,
                    x = "Calendar Year",
                    y = "Incidence Rate per 1,000 person years") +
      ggplot2::theme_minimal() +
      ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 90, hjust = 1))
    
    return(incidenceRate)
    
  }