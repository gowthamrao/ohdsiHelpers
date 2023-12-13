summarizeNumber <-
  function(dataFrame,
           number,
           name = number,
           groupBy = NULL,
           countDistinctOccurrencesOf = NULL,
           keepPositive = FALSE) {
    # Error handling for column names
    requiredColumns <-
      c(number, groupBy, countDistinctOccurrencesOf)
    missingColumns <-
      requiredColumns[!requiredColumns %in% names(dataFrame)]
    if (length(missingColumns) > 0) {
      stop("Missing columns in data frame: ",
           paste(missingColumns, collapse = ", "))
    }
    
    if (keepPositive) {
      dataFrame <- dataFrame |> dplyr::filter(!!rlang::ensym(number) > 0)
    }
    
    # Grouping (if required)
    if (!is.null(groupBy)) {
      groupBySymbols <- rlang::syms(groupBy)
      dataFrame <- dataFrame |> dplyr::group_by(!!!groupBySymbols)
    }
    
    # Define summarizing functions with prefixed names
    summarizingFunctions <- list(
      count = ~ dplyr::n(),
      mean = ~ mean(., na.rm = TRUE),
      median = ~ median(., na.rm = TRUE),
      sum = ~ sum(., na.rm = TRUE),
      min = ~ min(., na.rm = TRUE),
      max = ~ max(., na.rm = TRUE),
      first = ~ dplyr::first(.),
      last = ~ dplyr::last(.),
      sd = ~ sd(., na.rm = TRUE),
      IQR = ~ IQR(., na.rm = TRUE)
    )
    
    # Adding quantiles
    quantiles <-
      c(0.01, seq(from = 0.05, to = 0.95, by = 0.05), 0.99)
    for (q in quantiles) {
      summarizingFunctions[[paste0("quantile", q * 100)]] <-
        ~ quantile(., q, na.rm = TRUE)
    }
    
    # Apply summarizing functions using across
    summaryDataFrame <-
      dataFrame |>
      dplyr::summarize(across(all_of(number),
                              .fns = summarizingFunctions,
                              .names = "{name}_{.fn}"))
    
    # Count Distinct Occurrences (if required)
    if (!is.null(countDistinctOccurrencesOf)) {
      distinctCountColName <- paste0(name, "_ndist")
      summaryDataFrame[[distinctCountColName]] <- dataFrame |>
        dplyr::summarise(dplyr::n_distinct(!!rlang::ensym(countDistinctOccurrencesOf))) |>
        dplyr::pull(1)
    }
    
    # Ungroup (if grouped)
    if (!is.null(groupBy)) {
      summaryDataFrame <- summaryDataFrame |> dplyr::ungroup()
    }
    
    return(summaryDataFrame)
  }
