#' Function to trace all paths in a hierarchical parent-child relationship using depth-first search (DFS)
#'
#' This function explores a dataframe representing a hierarchy with parent and child IDs. It employs DFS
#' to trace and record all paths from root nodes to all reachable leaf nodes in the hierarchy.
#'
#' @param df A dataframe with columns 'parentId' and 'childId' indicating the hierarchical relationships.
#' @return A list of data frames, each containing a unique path from a root to leaves.
#' @export
tracePaths <- function(df) {
  roots <- df |>
    dplyr::filter(!is.na(parentId)) |>
    dplyr::pull(parentId) |>
    unique() |>
    sort()
  
  paths <- lapply(roots, function(root) {
    tracePath(df, root)
  })
  
  paths <- do.call(dplyr::bind_rows, paths) |>
    dplyr::arrange(parentId, pathId, sequence, childId)
  
  return(paths)
}

#' Helper function to trace paths for a given root using DFS
#'
#' This function constructs paths for a specified root by performing a DFS on a graph represented as a dataframe.
#'
#' @param df A dataframe representing edges with 'parentId' and 'childId' columns.
#' @param root The root node from which to start tracing paths.
#' @return A data frame representing all paths originating from the root.
tracePath <- function(df, root) {
  # Convert the dataframe to a list of edges
  edges <- split(df$childId, df$parentId) |>
    lapply(function(x)
      x[!is.na(x)])
  
  # Initialize result list
  paths <- list()
  
  # Helper function to perform DFS
  dfs <- function(node, path) {
    path <- c(path, node)
    children <- edges[[as.character(node)]]
    if (is.null(children)) {
      paths[[length(paths) + 1]] <<- path
    } else {
      for (child in children) {
        dfs(child, path)
      }
    }
  }
  
  # Start DFS from the root
  dfs(root, c())
  
  # Format paths into a data frame
  path_df_list <- lapply(seq_along(paths), function(i) {
    tibble::tibble(
      parentId = paths[[i]][1],
      pathId = i,
      childId = paths[[i]],
      sequence = seq_along(paths[[i]])
    ) |>
      dplyr::relocate(parentId, pathId, childId, sequence)
  })
  
  path_df <- do.call(dplyr::bind_rows, path_df_list)
  
  return(path_df)
}
