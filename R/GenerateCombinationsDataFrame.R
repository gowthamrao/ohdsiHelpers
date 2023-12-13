#' @export
generateCombinationsDataFrame <-
  function(numbers, combinationSizes = length(numbers)) {
    resultDF <- c()
    
    # Iterate over each combination size
    for (size in (1:combinationSizes)) {
      # Generate combinations for the current size
      comb <- combn(numbers, size)
      combinationSubId <- rep(1:ncol(comb), each = size)
      numbersVec <- as.vector(comb)
      sizeVec <- rep(size, length(numbersVec))
      
      # Create a dataframe for the current combination size
      resultDF[[size]] <-
        data.frame(combinationSubId,
                   combinationSize = sizeVec,
                   numbers = numbersVec)
    }
    
    resultDF <- dplyr::bind_rows(resultDF)
    allSubId <- resultDF |>
      dplyr::select(combinationSubId,
                    combinationSize) |>
      dplyr::distinct() |>
      dplyr::arrange(combinationSubId,
                     combinationSize) |>
      dplyr::mutate(combinationId = dplyr::row_number())
    
    resultDF <- allSubId |>
      dplyr::inner_join(resultDF,
                        by = c("combinationSubId",
                               "combinationSize")) |>
      dplyr::relocate(combinationId,
                      combinationSubId,
                      combinationSize) |>
      dplyr::tibble() |>
      dplyr::arrange(combinationId,
                     combinationSubId,
                     combinationSize)
    return(resultDF)
  }