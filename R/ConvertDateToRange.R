#' @export
convertDateToRange <- function(dates) {
  calendarRange <- dplyr::bind_rows(
    dates |>
      dplyr::mutate(
        startDate = min(date),
        endDate = max(date),
        type = "x"
      ),
    dates |>
      dplyr::mutate(
        startDate = lubridate::floor_date(date, "month"),
        endDate = lubridate::ceiling_date(date, "month") - 1,
        type = "m"
      ) |>
      dplyr::distinct(),
    dates |>
      dplyr::mutate(
        startDate = lubridate::floor_date(date, "quarter"),
        endDate = lubridate::ceiling_date(date, "quarter") - 1,
        type = "q"
      ) |>
      dplyr::distinct(),
    dates |>
      dplyr::mutate(
        startDate = lubridate::floor_date(date, "year"),
        endDate = lubridate::ceiling_date(date, "year") - 1,
        type = "y"
      ) |>
      dplyr::distinct()
  ) |>
    dplyr::distinct() |>
    dplyr::arrange(
      type,
      startDate,
      endDate
    ) |>
    dplyr::select(
      type,
      startDate,
      endDate
    )


  return(calendarRange)
}
