# Function to compare two lists and return a tibble
#' @export
compareLists <- function(list1, list2) {
  # Get paths for both lists
  paths1 <- traverseList(list1)
  paths2 <- traverseList(list2)

  # Combine and deduplicate paths
  allPaths <- unique(c(paths1, paths2))

  # Initialize result tibble
  result <- dplyr::tibble(
    fullPath = character(0),
    presentIn1 = numeric(0),
    presentIn2 = numeric(0)
  )

  # Populate the tibble
  for (path in allPaths) {
    presentIn1 <- as.numeric(path %in% paths1)
    presentIn2 <- as.numeric(path %in% paths2)

    newRow <- dplyr::tibble(
      fullPath = path,
      presentIn1 = presentIn1,
      presentIn2 = presentIn2
    )
    result <- rbind(result, newRow)
  }

  return(result)
}
