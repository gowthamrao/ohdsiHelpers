#' @export
convertSentenceToCamelCase <- function(strings) {
  # A function to clean and convert each string
  cleanConvert <- function(s) {
    # Remove invalid characters, could be adjusted depending on what is considered invalid
    s <- gsub("[^a-zA-Z0-9 ]", "", s)
    # Convert to lower case
    s <- tolower(s)
    # Split by space
    words <- strsplit(s, " ")[[1]]
    # Capitalize first letter of each word except the first one
    if (length(words) > 1) {
      words[-1] <- sapply(words[-1], tools::toTitleCase)
    }
    # Combine words without spaces
    paste0(words, collapse = "")
  }
  
  string <- c()
  for (i in (1:length(strings)))  {
    string <- c(string, cleanConvert(strings[[i]]))
  }
  return(string)
}