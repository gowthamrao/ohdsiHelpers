# Function to convert comma-separated string to an array of integers
#' @export
commaSeparaedStringToIntArray <- function(inputString) {
  # Split the string into elements based on commas
  stringElements <- strsplit(inputString, ",")[[1]]
  # Remove empty elements
  stringElements <- stringElements[stringElements != ""]
  # Convert elements to integer
  integerArray <- as.integer(stringElements)
  return(integerArray)
}
