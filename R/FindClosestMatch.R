# Function to find closest match
#' @export
findClosestMatch <- function(sourceString, targetVector) {
  distances <- stringdist::stringdist(sourceString, targetVector)
  targetVector[which.min(distances)]
}


# Function to perform fuzzy string join on two data frames
#' @export
fuzzyStringJoinDataFrame <- function(df1, df2, field1, field2) {
  library(tidyverse)
  library(stringdist)
  
  # Calculate the closest matches using stringdist and include match score
  findClosestMatch <- function(sourceString, targetVector) {
    distances <- stringdist::stringdist(sourceString, targetVector)
    minIndex <- which.min(distances)
    score <- 1 - (distances[minIndex] / max(nchar(sourceString), nchar(targetVector[minIndex])))
    return(c(match = targetVector[minIndex], score = score))
  }
  
  # Map each entry in df1's field1 to the closest entry in df2's field2 and include scores
  closestMatches <- dplyr::tibble(field1 = df1[[field1]]) |> 
    dplyr::rowwise() |> 
    dplyr::mutate(
      closestField2 = findClosestMatch(field1, df2[[field2]])[1],
      matchScore = as.numeric(findClosestMatch(field1, df2[[field2]])[2])
    ) |> 
    dplyr::ungroup()
  
  # Join the closest matches with df2 to retrieve all corresponding columns
  mappedData <- closestMatches |> 
    dplyr::left_join(df2, by = c("closestField2" = field2))
  
  # Join the mapped data back with df1
  finalResult <- mappedData |> 
    dplyr::right_join(df1, by = c("field1" = field1))
  
  # Rename columns to maintain the original naming convention from df1 and df2
  finalResult <- finalResult |> 
    dplyr::rename_with(.cols = everything(), .fn = ~gsub("field1", field1, .)) |> 
    dplyr::rename_with(.cols = everything(), .fn = ~gsub("closestField2", field2, .))
  
  finalResult <- df1 |> 
    dplyr::left_join(finalResult)
  
  return(finalResult)
}

