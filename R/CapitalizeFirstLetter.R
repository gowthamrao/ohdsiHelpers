#' @export
capitalizeFirstLetter <- function(text) {
  # Vectorize the core functionality to handle vector inputs properly
  capitalizer <- Vectorize(function(sentence) {
    if (is.na(sentence) || sentence == "") {
      return(sentence)
    }
    # Split text into sentences
    sentences <-
      unlist(strsplit(sentence, "(?<=[.!?])\\s*", perl = TRUE))
    # Capitalize the first letter of each sentence
    capitalizedSentences <- sapply(sentences, function(s) {
      paste0(toupper(substring(s, 1, 1)), substring(s, 2))
    })
    # Combine sentences back into a single string
    paste(capitalizedSentences, collapse = " ")
  })
  
  return(capitalizer(text))
}