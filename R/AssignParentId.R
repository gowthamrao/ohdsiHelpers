#' Assign Parent ID Based on Name Substring Matching
#'
#' This function assigns `parentId` for each entry in a data frame based on the logic that the
#' parent ID should be the ID of the entry that has the name which is a substring of the current
#' name, is shorter than the current name, and has the most characters matching contiguously
#' with the current name.
#'
#' @param df A data frame with at least two columns: `id` and `name`.
#' @return A data frame with an additional column `parentId` containing the assigned parent IDs.
#' @export
assignParentId <- function(df) {
  # Load checkmate package
  if (!requireNamespace("checkmate", quietly = TRUE)) {
    stop("The 'checkmate' package is required but not installed.")
  }
  
  # Assertions to check the input data frame
  checkmate::assertDataFrame(df)
  checkmate::assertNames(names(df), must.include = c("id", "name"))
  checkmate::assertIntegerish(
    df$id,
    any.missing = FALSE,
    all.missing = FALSE,
    unique = TRUE
  )
  checkmate::assertCharacter(
    df$name,
    any.missing = FALSE,
    all.missing = FALSE,
    min.chars = 1
  )
  
  df$parentId <- NA
  
  for (i in seq_len(nrow(df))) {
    currentName <- df$name[i]
    potentialParents <-
      df[df$name != currentName &
           nchar(df$name) < nchar(currentName), ]
    
    bestMatchId <- NA
    bestMatchLength <- 0
    
    for (j in seq_len(nrow(potentialParents))) {
      parentName <- potentialParents$name[j]
      if (grepl(parentName, currentName, fixed = TRUE)) {
        matchLength <- nchar(parentName)
        if (matchLength > bestMatchLength) {
          bestMatchLength <- matchLength
          bestMatchId <- potentialParents$id[j]
        }
      }
    }
    
    df$parentId[i] <- bestMatchId
  }
  
  df <- df |>
    dplyr::mutate(parentId = dplyr::if_else(
      condition = is.na(parentId),
      true = id,
      false = parentId
    )) |>
    dplyr::left_join(df |>
                       dplyr::select(id, name) |>
                       dplyr::rename("parentId" = id,
                                     "parentName" = name)) |>
    dplyr::mutate(subsetName = dplyr::if_else(
      condition = parentName == name,
      true = name,
      false = paste0(
        strrep("-", nchar(parentName)),
        stringr::str_remove(name, stringr::fixed(parentName))
      )
    )) |>
    dplyr::arrange(name,
                   parentName,
                   parentId,
                   name,
                   subsetName,
                   id) |>
    dplyr::mutate(rn = dplyr::row_number()) |>
    dplyr::relocate(rn,
                    parentId,
                    id,
                    subsetName)
  
  # Function to find the root ID
  findRootId <- function(id, data) {
    parent <- data$parentId[data$id == id]
    if (is.na(parent) | parent == id) {
      return(id)
    } else {
      return(findRootId(parent, data))
    }
  }
  
  # Vectorize the function to apply over a vector of ids
  findRootIdVectorized <-
    Vectorize(findRootId, vectorize.args = "id")
  
  # Add rootId column
  df$rootId <- findRootIdVectorized(df$id, df)
  
  df <- df |>
    dplyr::mutate(parentId = dplyr::if_else(
      condition = (id == parentId),
      true = NA,
      false = parentId
    ))
  
  return(df)
}
