#' @export
plotTrendCurve <- function(dataTibble, dateColumn,
                           smoothingMethod = "auto",
                           showStandardError = TRUE,
                           outputFilename = NULL,
                           plotTitle = "Trend Curve",
                           xAxisLabel = "Date",
                           yAxisLabel = "Volume",
                           yScaleTransform = "identity",
                           ggplotTheme = ggplot2::theme_minimal(),
                           dateLabelDf = NULL,
                           cumulative = FALSE,
                           showRawValues = FALSE,
                           showCumulativeValues = FALSE,
                           cumulativeYPosition = -10,
                           cumulativeJitter = 3,
                           dateRounding = "none") {
  # Check if required packages are installed
  if (!requireNamespace("ggplot2", quietly = TRUE)) {
    stop("Package 'ggplot2' is required.")
  }

  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("Package 'dplyr' is required.")
  }

  if (!requireNamespace("lubridate", quietly = TRUE)) {
    stop("Package 'lubridate' is required.")
  }

  if (!requireNamespace("checkmate", quietly = TRUE)) {
    stop("Package 'checkmate' is required.")
  }

  # Validate the dataTibble to make sure it has the required dateColumn
  checkmate::assert_subset(dateColumn, choices = names(dataTibble))

  # Validate the dateLabelDf if provided
  if (!is.null(dateLabelDf)) {
    checkmate::assert_subset(c("date", "label"), choices = names(dateLabelDf))
  }

  # Round the date if requested
  if (dateRounding != "none") {
    dataTibble <- dataTibble |>
      dplyr::mutate(!!rlang::sym(dateColumn) := lubridate::floor_date(!!rlang::sym(dateColumn), unit = dateRounding))
  }
  
  # Pre-aggregate data to get the counts for each date
  aggregatedData <- dplyr::count(dataTibble, !!rlang::sym(dateColumn), name = "volume")

  # Prepare the cumulative data, regardless of the `cumulative` setting, if `showCumulativeValues` is TRUE
  if (showCumulativeValues) {
    cumulativeData <- aggregatedData |>
      dplyr::arrange(!!rlang::sym(dateColumn)) |>
      dplyr::mutate(volume = cumsum(.data$volume))
  }

  # If the plot itself should be cumulative, replace aggregatedData with its cumulative version
  if (cumulative) {
    aggregatedData <- cumulativeData
  }

  # Create the base ggplot2 object
  plot <- ggplot2::ggplot(aggregatedData, ggplot2::aes_string(x = dateColumn, y = "volume")) +
    ggplot2::scale_y_continuous(trans = yScaleTransform) +
    ggplot2::labs(
      title = plotTitle,
      x = xAxisLabel,
      y = yAxisLabel
    ) +
    ggplotTheme

  # Add raw values as light gray dots if requested
  if (showRawValues) {
    plot <- plot + ggplot2::geom_point(color = "lightgray")
  }

  # Add either the smoothing layer or the line layer based on smoothingMethod
  if (!is.null(smoothingMethod)) {
    plot <- plot + ggplot2::geom_smooth(method = smoothingMethod, se = showStandardError)
  } else {
    plot <- plot + ggplot2::geom_line()
  }

  # Add date annotations if dateLabelDf is provided
  if (!is.null(dateLabelDf)) {
    maxY <- max(aggregatedData$volume, na.rm = TRUE)
    plot <- plot + ggplot2::geom_text(
      data = dateLabelDf,
      ggplot2::aes(x = date, y = maxY, label = label),
      nudge_y = 0.1,
      check_overlap = TRUE
    )
  }

  # Add cumulative count annotations below the x-axis if requested
  if (showCumulativeValues) {
    # Extract the last date of each month
    lastDates <- cumulativeData |>
      dplyr::mutate(month = lubridate::floor_date(!!rlang::sym(dateColumn), "month")) |>
      dplyr::group_by(month) |>
      dplyr::slice_tail(n = 1) |>
      dplyr::ungroup()

    # Add jitter to the y-position to avoid overlap
    jitteredYPosition <- cumulativeYPosition + runif(nrow(lastDates), -cumulativeJitter, cumulativeJitter)

    # Add the annotations
    plot <- plot + ggplot2::annotate("text",
      x = lastDates[[dateColumn]], y = jitteredYPosition,
      label = scales::comma(lastDates$volume),
      size = 3, color = "lightgray"
    )
  }

  # Display the plot
  print(plot)

  # Save the plot to a file if outputFilename is provided
  if (!is.null(outputFilename)) {
    ggplot2::ggsave(outputFilename, plot)
  }
}
