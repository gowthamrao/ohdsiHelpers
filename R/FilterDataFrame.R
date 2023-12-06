#' @export
filterDataFrame <- function(data, ...) {
  # Check if 'dplyr' is installed
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("The 'dplyr' package is not installed. Please install it to use this function.")
  }
  
  # Capture the filtering conditions
  conditions <- rlang::quos(...)
  
  # Check for missing variables and incompatible data types
  for (cond in conditions) {
    # Extract the expression
    expr <- rlang::quo_get_expr(cond)
    
    # Extract variable name
    if (length(expr) < 3) {
      stop("Invalid condition: ", rlang::quo_text(cond))
    }
    varName <- all.vars(expr)[1]
    
    # Check if variable exists in the data frame
    if (!varName %in% names(data)) {
      stop("Variable '", varName, "' not found in the data frame.")
    }
    
    # Evaluate the right-hand side of the expression in the context of the data frame
    rhsValue <- tryCatch({
      eval(expr[[3]], envir = data)
    }, error = function(e) {
      stop("Error evaluating the condition '", rlang::quo_text(cond), "': ", e$message)
    })
    
    # Check data type compatibility
    if (!is.na(rhsValue) && !inherits(rhsValue, class(data[[varName]]))) {
      stop("Incompatible data types for variable '", varName, "'. Expected ", class(data[[varName]]), ", got ", class(rhsValue), ".")
    }
  }
  
  # Apply the filters
  filteredData <- dplyr::filter(data, !!!conditions)
  
  return(filteredData)
}
