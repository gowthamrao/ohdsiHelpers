#' Create a Hierarchical Data Tree from a Data Frame
#'
#' This function converts a flat data frame representing a hierarchical structure (with parent-child relationships) into a `Node` object from the `data.tree` package.
#'
#' @param df A data frame containing the hierarchical data.
#' @param childField (character) The name of the column in `df` representing the child identifiers (e.g., "id").
#' @param parentField (character) The name of the column in `df` representing the parent identifiers (e.g., "parentId").
#'
#' @return A `Node` object from the `data.tree` package representing the hierarchical structure. The node names are taken from the "name" column (if present) or the child ID column.
#'
#' @details This function assumes that the input data frame `df` contains at least three columns:
#'   - `childField`: A unique identifier for each child node.
#'   - `parentField`: The identifier of the parent node for each child.
#'   - `name`: A descriptive name for each node (optional). If not provided, the child ID will be used as the node name.
#'
#' The function validates the input, checks for the presence of a root node, and constructs the hierarchical tree using the `data.tree::as.Node` function.
#' @export
createDataTree <-
  function(df,
           childField,
           parentField,
           numberOfLevels = 999,
           rootName = "RootNode") {
    
    hierarchicalData <-
      createHierarchy(
        df = df,
        childField = childField,
        parentField = parentField,
        maxLevels = numberOfLevels
      )
    
    # Check if data.tree package is installed
    if (!requireNamespace("data.tree", quietly = TRUE)) {
      stop(
        "The 'data.tree' package is required but not installed. Please install it with install.packages('data.tree')."
      )
    }
    
    # Validate dataframe structure
    if (!is.data.frame(df)) {
      stop("Input must be a data frame.")
    }
    
    # Check if specified columns exist
    required_cols <- c(childField, parentField, "name")
    missing_cols <- setdiff(required_cols, names(df))
    if (length(missing_cols) > 0) {
      stop("Missing required columns: ",
           paste(missing_cols, collapse = ", "))
    }
    
    # Convert columns to character for consistency
    df[[childField]] <- as.character(df[[childField]])
    df[[parentField]] <- as.character(df[[parentField]])
    
    # Check for missing root node
    if (!any(is.na(df[[parentField]]))) {
      stop("No root node found (parentId should be NA for the root).")
    }
    
    # Convert to data.tree structure
    # Check if rootName is reserved or empty, if so, use default root name
    if (rootName %in% data.tree::NODE_RESERVED_NAMES_CONST ||
        rootName == "") {
      rootName <- "RootNode"
    }
    
    df$pathString <-
      with(df, paste(rootName, get(parentField), get(childField), sep = "/"))
    tree <- data.tree::as.Node(df)
    
    return(tree)
  }