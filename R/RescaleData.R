#' Rescale data within a DataFrame based on specified methods
#'
#' This function applies various rescaling methods to a selected field within a data frame, optionally
#' grouped by one or more columns. Rescaling methods include min-max scaling, absolute change, percent change,
#' logarithmic transformation, z-score normalization, and robust z-score using median absolute deviation.
#'
#' @param data A DataFrame containing the data to be rescaled.
#' @param observedField The name of the column in `data` whose values are to be rescaled.
#' @param expectedField Optional; the name of the column against which rescaling calculations such as
#'        absolute or percent changes are to be computed. Required if `rescaleMethod` is either "absoluteChange"
#'        or "percentChange".
#' @param groupBy Optional; vector of column names in `data` used for grouping data before applying the
#'        rescaling. If NULL, the entire dataset is treated as a single group.
#' @param rescaleMethod A character string specifying the method of rescaling to apply. Supported methods
#'        are "none" (returns original values), "minMax" (min-max normalization), "absoluteChange" (difference from `expectedField`),
#'        "percentChange" (percentage difference from `expectedField`), "log" (logarithmic scale transformation),
#'        "zScore" (z-score normalization), and "robust" (robust z-score using median absolute deviation).
#'        Default is "none".
#'
#' @return A DataFrame with the `observedField` rescaled according to the specified method.
#'
#' @export
rescaleData <-
  function(data,
           observedField,
           expectedField = NULL,
           groupBy = NULL,
           rescaleMethod = "none") {
    # Expanded list of acceptable rescaling methods
    acceptableMethods <- c(
      "none",
      "minMax",
      "absoluteChange",
      "percentChange",
      "log",
      "zScore",
      "robust"
    )
    
    # Check if the provided rescale method is valid
    if (!rescaleMethod %in% acceptableMethods) {
      stop("rescaleMethod must be one of the following: ",
           toString(acceptableMethods))
    }
    
    # Ensure the input data is a data frame
    if (!is.data.frame(data)) {
      stop("Data must be a data.frame.")
    }
    
    # Ensure the column for observedField rescaling exists in the data
    if (!observedField %in% names(data)) {
      stop("observedField column not found in data.")
    }
    
    # Check if the groupBy parameter is provided and valid
    if (!is.null(groupBy) && !all(groupBy %in% names(data))) {
      stop("Group by column(s) not found in data.")
    }
    
    # Verify that expectedField is provided for methods that require it
    methodsRequiringBaseValue <-
      c("absoluteChange", "percentChange")
    if (rescaleMethod %in% methodsRequiringBaseValue &&
        (is.null(expectedField) ||
         !expectedField %in% names(data))) {
      stop(
        "expectedField is required and must be a valid column in the data for the selected rescaleMethod."
      )
    }
    
    # Only proceed with rescaling if a method other than 'none' is chosen
    if (rescaleMethod != "none") {
      # Create a grouping factor based on the groupBy columns, or default to one group if null
      groupFactor <- if (is.null(groupBy)) {
        rep(1, nrow(data))  # Single group if no grouping column provided
      } else {
        interaction(data[, groupBy])  # Multiple groups based on specified columns
      }
      
      # Split the data into groups based on the grouping factor
      groupedData <- split(data, groupFactor)
      
      # Apply the rescale method to each group and then combine them back
      data <- do.call(rbind, lapply(groupedData, function(group) {
        valueData <- group[[observedField]]
        baselineData <-
          if (!is.null(expectedField))
            group[[expectedField]]
        else
          NULL
        
        # Apply the chosen rescale method
        group[[observedField]] <- switch(
          rescaleMethod,
          minMax = {
            rangeVal <- range(valueData, na.rm = TRUE)
            - 1 + 2 * (valueData - rangeVal[1]) / (rangeVal[2] - rangeVal[1])
          },
          absoluteChange = valueData - baselineData,
          percentChange = {
            if (any(baselineData == 0)) {
              warning("Division by zero in percentChange calculation; returning NA for these cases.")
              100 * (valueData - baselineData) / ifelse(baselineData == 0, NA, baselineData)
            } else {
              100 * (valueData - baselineData) / baselineData
            }
          },
          log = {
            minValue <- min(valueData[valueData > 0], na.rm = TRUE)
            log10(pmax(valueData, ifelse(
              minValue > 0, minValue, 0.000000001
            )))
          },
          zScore = (valueData - mean(valueData, na.rm = TRUE)) / sd(valueData, na.rm = TRUE),
          robust = (valueData - median(valueData, na.rm = TRUE)) / stats::mad(valueData, na.rm = TRUE),
          valueData
        )
        group
      }))
    }
    
    # Return the modified or original data frame
    return(data)
  }




# 
# 
# library(testthat)
# 
# # Sample data for testing
# data <- data.frame(
#   observedField = c(2, 4, 6, 8, 10),
#   expectedField = c(1, 2, 3, 4, 5),
#   groupBy = c('A', 'A', 'B', 'B', 'B')
# )
# 
# 
# # Testing the 'none' transformation
# test_that("Testing none transformation", {
#   result <- rescaleData(data, "observedField", rescaleMethod = "none")
#   expect_equal(result$observedField, data$observedField)
# })
# 
# # Testing the 'minMax' transformation
# test_that("Testing minMax transformation", {
#   expected <- c(-1, -0.5, 0, 0.5, 1)
#   result <- rescaleData(data, "observedField", rescaleMethod = "minMax")
#   expect_equal(result$observedField, expected)
# })
# 
# # Testing the 'absoluteChange' transformation
# test_that("Testing absoluteChange transformation", {
#   expected <- c(1, 2, 3, 4, 5)
#   result <- rescaleData(data, "observedField", "expectedField", rescaleMethod = "absoluteChange")
#   expect_equal(result$observedField, expected)
# })
# 
# # Testing the 'percentChange' transformation
# test_that("Testing percentChange transformation", {
#   expected <- c(100, 100, 100, 100, 100)
#   result <- rescaleData(data, "observedField", "expectedField", rescaleMethod = "percentChange")
#   expect_equal(result$observedField, expected)
# })
# 
# # Testing the 'log' transformation
# test_that("Testing log transformation", {
#   expected <- log10(data$observedField)
#   result <- rescaleData(data, "observedField", rescaleMethod = "log")
#   expect_equal(result$observedField, expected)
# })
# 
# # Testing the 'zScore' transformation
# test_that("Testing zScore transformation", {
#   meanVal <- mean(data$observedField)
#   sdVal <- sd(data$observedField)
#   expected <- (data$observedField - meanVal) / sdVal
#   result <- rescaleData(data, "observedField", rescaleMethod = "zScore")
#   expect_equal(result$observedField, expected)
# })
# 
# # Testing the 'robust' transformation
# test_that("Testing robust transformation", {
#   medianVal <- median(data$observedField)
#   madVal <- mad(data$observedField)
#   expected <- (data$observedField - medianVal) / madVal
#   result <- rescaleData(data, "observedField", rescaleMethod = "robust")
#   expect_equal(result$observedField, expected)
# })


