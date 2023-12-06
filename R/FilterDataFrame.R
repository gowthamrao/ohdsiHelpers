#' @export
filterDataFrame <- function(data, ...) {
  # Load necessary libraries
  rlang::check_installed("dplyr", dependencies = TRUE)
  
  # Capture the filtering conditions
  conditions <- rlang::quos(...)
  
  # Check for missing variables and incompatible data types
  for (cond in conditions) {
    # Extract variable name and value from the condition
    varName <- all.vars(cond[[2]])[1]
    value <- eval(cond[[2]][[3]])
    
    # Check if variable exists in the data frame
    if (!varName %in% names(data)) {
      stop("Variable '", varName, "' not found in the data frame.")
    }
    
    # Check data type compatibility
    if (!is.na(value) && !inherits(value, class(data[[varName]]))) {
      stop("Incompatible data types for variable '", varName, "'. Expected ", class(data[[varName]]), ", got ", class(value), ".")
    }
  }
  
  # Apply the filters
  filteredData <- dplyr::filter(data, !!!conditions)
  
  return(filteredData)
}
