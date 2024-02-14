#' Generate Combinations of Array Elements
#'
#' This function generates all possible combinations of the elements in a given array.
#' For each combination, it creates a tibble row for every element in the array that is part of that combination.
#' The resulting tibble contains two columns: one for the individual elements and another for the combinations they appear in.
#'
#' @param array A character vector containing unique elements for which combinations are to be generated.
#'
#' @return A tibble with two columns: `array` containing the individual elements, and `combinations` containing the combinations.
#' @export
createCombinations <- function(array) {
  # Check if 'tibble' package is installed and load it
  if (!requireNamespace("tibble", quietly = TRUE))
    install.packages("tibble")
  
  # Initialize an empty tibble to store combinations
  resultDataFrame <- dplyr::tibble()
  
  # Loop over the length of the array to generate all possible combinations
  for (i in 1:length(array)) {
    # Generate combinations of size i
    combinations <- combn(array, i, simplify = FALSE)
    # Sort each combination and concatenate elements with commas
    sortedCombinations <- lapply(combinations, function(x) {
      paste(sort(x), collapse = ",")
    })
    
    # Loop over each combination
    for (j in (1:length(sortedCombinations))) {
      # Loop over each element in the array
      for (name in array) {
        # Check if the element is part of the current combination
        if (name %in% unlist(strsplit(sortedCombinations[[j]], ","))) {
          # Add a row to the tibble with the element and its combination
          resultDataFrame <-
            dplyr::bind_rows(
              resultDataFrame,
              tibble::tibble(array = name, combinations = sortedCombinations[[j]])
            ) |>
            dplyr::distinct()
        }
      }
    }
  }
  
  resultDataFrame <- resultDataFrame |>
    dplyr::inner_join(
      resultDataFrame |>
        dplyr::select(combinations) |>
        dplyr::distinct() |>
        dplyr::arrange() |>
        dplyr::mutate(id = dplyr::row_number()),
      by = "combinations"
    ) |>
    dplyr::arrange(id) |>
    dplyr::relocate(id,
                    combinations,
                    name)
  
  return(resultDataFrame)
}
