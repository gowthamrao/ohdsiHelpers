#' @export
prettyReportKabbleTable <- function(dataFrame,
                                    caption = "Table",
                                    captionStyle = "",
                                    align = "l",
                                    formatAsInteger = NULL,
                                    formatAsDecimals = NULL,
                                    formatAsPercent = NULL) {
  colnamesInDataFrame <- colnames(dataFrame)
  # Format columns as integers with commas
  if (!is.null(formatAsInteger)) {
    formatAsInteger <- intersect(
      colnamesInDataFrame,
      formatAsInteger
    )
    if (length(formatAsInteger) > 0) {
      for (col in formatAsInteger) {
        dataFrame[[col]] <- scales::label_comma()(dataFrame[[col]])
      }
    }
  }

  # Format columns as decimals
  if (!is.null(formatAsDecimals)) {
    formatAsDecimals <- intersect(
      colnamesInDataFrame,
      formatAsDecimals
    )
    if (length(formatAsDecimals) > 0) {
      for (col in formatAsDecimals) {
        dataFrame[[col]] <-
          scales::label_number(accuracy = 0.1)(dataFrame[[col]])
      }
    }
  }

  # Format columns as integers with commas
  if (!is.null(formatAsPercent)) {
    formatAsPercent <- intersect(
      colnamesInDataFrame,
      formatAsPercent
    )
    if (length(formatAsPercent) > 0) {
      for (col in formatAsPercent) {
        dataFrame[[col]] <-
          scales::label_percent(accuracy = 0.01)(dataFrame[[col]])
      }
    }
  }

  colnames(dataFrame) <-
    SqlRender::camelCaseToTitleCase(colnames(dataFrame))

  table <- knitr::kable(dataFrame,
    caption = caption,
    format = "html",
    align = align
  )

  # Apply caption style if specified
  if (!is.null(captionStyle)) {
    if (tolower(captionStyle) %in% c("html", "latex")) {
      # For HTML or LaTeX, use the 'caption' package
      table <- table |>
        kableExtra::kable_styling(
          latex_options = c("hold_position"),
          full_width = FALSE,
          position = "center"
        ) |>
        kableExtra::add_header_above(c(" " = ncol(.data$dfFormatted), . = captionStyle))
    } else {
      # For other styles, assume it's a CSS class for HTML output
      table <- table |>
        kableExtra::kable_styling(
          bootstrap_options = c("striped", "hover"),
          html_font = captionStyle
        )
    }
  } else {
    table <- table |>
      kableExtra::kable_styling(bootstrap_options = c("striped", "hover"))
  }

  return(table)
}
