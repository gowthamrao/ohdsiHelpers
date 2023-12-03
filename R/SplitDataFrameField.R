#' @export
splitDataFrameField <- function(data,
                                fieldName,
                                splitChar, 
                                newFieldNames) {
  # Split the column into multiple columns based on the split character
  splitValues <- strsplit(data[[fieldName]], splitChar)
  splitValues <- sapply(splitValues, function(x) {
    if (length(x) < length(newFieldNames)) {
      c(x, rep("", length(newFieldNames) - length(x)))
    } else {
      x[1:length(newFieldNames)]
    }
  })
  
  # Add the new columns to the dataframe
  newColumns <-
    setNames(as.data.frame(t(splitValues)), newFieldNames)
  
  if (fieldName %in% c(newFieldNames)) {
    # Remove the original column
    data <- data |> 
      dplyr::select(-fieldName)
  }
  
  newData <- cbind(data, newColumns)
  
  # Return the modified dataframe
  return(newData |> 
           dplyr::tibble())
}