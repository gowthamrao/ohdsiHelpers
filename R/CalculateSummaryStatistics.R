#' @export
calculateSummaryStatistics <-
  function(df, value = "value", group = "group") {
    # Ensure correct package references
    dataFrame <- df |>
      dplyr::mutate(!!group := .data[[group]],!!value := .data[[value]]) |>
      dplyr::select(.data[[group]],
                    .data[[value]])
    
    # Helper function to calculate mode
    calculateMode <- function(x) {
      ux <- unique(x)
      ux[which.max(tabulate(match(x, ux)))]
    }
    
    output <- dataFrame |>
      dplyr::group_by(.data[[group]]) |>
      dplyr::summarize(
        mean = mean(.data[[value]], na.rm = TRUE),
        sd = sd(.data[[value]], na.rm = TRUE),
        median = median(.data[[value]], na.rm = TRUE),
        p5 = quantile(.data[[value]], 0.05, na.rm = TRUE),
        p25 = quantile(.data[[value]], 0.25, na.rm = TRUE),
        p75 = quantile(.data[[value]], 0.75, na.rm = TRUE),
        p95 = quantile(.data[[value]], 0.95, na.rm = TRUE),
        mode = calculateMode(.data[[value]]),
        count = n(),
        count_distinct = n_distinct(.data[[value]])
      ) |>
      tidyr::pivot_longer(
        cols = -all_of(group),
        names_to = "statistic",
        values_to = "value"
      )
    
    return(output)
  }