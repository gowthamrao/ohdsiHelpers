#' @export
removeLeadingCharacters <- function(inputString, pattern = "-| ") {
  # Escape special regex characters in the pattern, except for hyphen which will be moved to the end
  escapedPattern <- gsub("([][{}()+*^$|\\.?])", "\\\\\\1", pattern)
  # Place hyphen at the end of character set to avoid range specification issues
  escapedPattern <- gsub("-", "", escapedPattern, fixed = TRUE) # Remove existing hyphens
  regexPattern <- paste0("^[", escapedPattern, "-]+") # Add hyphen at the end of the set
  resultString <- sub(pattern = regexPattern, replacement = "", x = inputString)
  return(resultString)
}
