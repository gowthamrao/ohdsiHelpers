# Function to recursively traverse the list and store paths
#' @export
traverseList <-
  function(lst,
           currentPath = "",
           paths = list(),
           seperator = "/") {
    for (i in seq_along(lst)) {
      extractedName <- names(lst)[i]

      if (is.null(extractedName)) {
        name <- paste0("[[", i, "]]")
      } else {
        name <- extractedName
      }

      if (seperator == "$" && is.null(extractedName)) {
        newPath <- paste0(currentPath, name)
      } else {
        newPath <- paste0(currentPath, seperator, name)
      }

      if (is.list(lst[[i]])) {
        paths <-
          traverseList(
            lst = lst[[i]],
            currentPath = newPath,
            paths = paths,
            seperator = seperator
          )
      }

      newPath <- removeFirstCharacter(
        inputString = newPath,
        charToRemove = seperator
      )
      paths <- c(paths, newPath)
    }
    return(paths)
  }
