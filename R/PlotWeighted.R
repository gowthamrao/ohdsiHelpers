#' @export
plotWeighted <- function(data,
                         x = "value",
                         y = "weight",
                         mainTitle = "Weighted Plot",
                         xLabel = "Values",
                         yLabel = "Weights",
                         plotColor = "blue",
                         plotType = "histogram",
                         binwidth = NULL,
                         useScinot = FALSE,
                         plotTheme = "theme_minimal") {
  if (!useScinot) {
    # Temporarily increase the penalty for scientific notation
    oldScipen <- getOption("scipen")
    options(scipen = 999)
  }

  if (!is.data.frame(data)) {
    data <- dplyr::tibble(
      value = data,
      weight = 1
    )
    x <- "value"
    y <- "weight"
  }

  if (!x %in% names(data)) {
    stop("Variable for x-axis is not found in the data.")
  }

  if (!y %in% names(data)) {
    stop("Variable for weight is not found in the data.")
  }

  if (plotType == "histogram") {
    plot <-
      ggplot2::ggplot(data, ggplot2::aes_string(x = x, weight = y)) +
      ggplot2::geom_histogram(fill = plotColor, binwidth = binwidth) +
      ggplot2::ggtitle(mainTitle) +
      ggplot2::xlab(xLabel) +
      ggplot2::ylab(yLabel) +
      ggplot2::scale_x_continuous(labels = scales::comma) +
      ggplot2::scale_y_continuous(labels = scales::comma) +
      do.call(plotTheme, list())
  } else if (plotType == "bar") {
    dataSummarised <- data |>
      dplyr::group_by(.data[[x]]) |>
      dplyr::summarise(weight = sum(.data[[y]]))

    plot <-
      ggplot2::ggplot(dataSummarised, ggplot2::aes(!!rlang::sym(x), y = weight)) +
      ggplot2::geom_bar(
        stat = "identity",
        fill = plotColor
      ) +
      ggplot2::ggtitle(mainTitle) +
      ggplot2::xlab(xLabel) +
      ggplot2::ylab(yLabel) +
      ggplot2::scale_x_continuous(labels = scales::comma) +
      ggplot2::scale_y_continuous(labels = scales::comma) +
      do.call(plotTheme, list())
  } else {
    stop("Unsupported plot type. Supported types are 'histogram' and 'bar'.")
  }

  return(plot)

  if (!useScinot) {
    # Reset the scientific notation penalty
    options(scipen = oldScipen)
  }
}
