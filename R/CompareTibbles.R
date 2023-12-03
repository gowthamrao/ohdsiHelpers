#' @export
compareTibbles <- function(tibble1, tibble2) {
  # Initialize result list
  result <- list()
  
  # Check if columns are identical (ignoring order)
  columns1 <- sort(names(tibble1))
  columns2 <- sort(names(tibble2))
  
  # Find additional columns
  additionalColumns1 <- setdiff(columns1, columns2)
  additionalColumns2 <- setdiff(columns2, columns1)
  
  result$additionalColumnsInFirst <- additionalColumns1
  result$additionalColumnsInSecond <- additionalColumns2
  
  # If columns are not identical, return result
  if (!identical(columns1, columns2)) {
    result$identical <- FALSE
    message("The two tibbles have different columns")
    return(result)
  }
  
  # Sort columns
  tibble1 <- tibble1[, columns1]
  tibble2 <- tibble2[, columns2]
  
  # Sort rows
  tibble1 <- tibble1[do.call(order, tibble1), ]
  tibble2 <- tibble2[do.call(order, tibble2), ]
  
  # Compare rows
  identicalRows <- identical(tibble1, tibble2)
  result$identical <- identicalRows
  
  if (identicalRows) {
    return(result)
  }
  
  # Find additional rows
  additionalRows1 <- nrow(tibble1) - nrow(tibble2)
  additionalRows2 <- nrow(tibble2) - nrow(tibble1)
  
  result$additionalRowsInFirst <- additionalRows1
  result$additionalRowsInSecond <- additionalRows2
  
  # Find rows present in first but not in second
  diffRows1 <- setdiff(tibble1, tibble2)
  
  # Find rows present in second but not in first
  diffRows2 <- setdiff(tibble2, tibble1)
  
  result$presentInFirstNotSecond <- diffRows1
  result$presentInSecondNotFirst <- diffRows2
  
  return(result)
}

