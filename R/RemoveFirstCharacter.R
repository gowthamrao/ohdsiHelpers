removeFirstCharacter <- function(inputString, charToRemove) {
  # Check if the first character matches the character to remove
  if (substr(inputString, 1, 1) == charToRemove) {
    # Remove the first character
    return(substr(inputString, 2, nchar(inputString)))
  } else {
    # Return the original string if the first character doesn't match
    return(inputString)
  }
}
