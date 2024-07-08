#' Extract PDF document content and metadata as a JSON object.
#'
#' This function extracts both metadata and text content from a PDF file,
#' organizes the text into structured paragraphs, and merges text blocks to improve coherence.
#' It returns the data as a structured JSON object.
#'
#' @param pdfPath The file path to the PDF document.
#'
#' @return A JSON object containing the PDF metadata and structured text.
#' @export
getPdfDocumentAsJson <- function(pdfPath) {
  # Load necessary libraries
  require(pdftools)
  require(jsonlite)
  
  # Check if the file exists
  if (!file.exists(pdfPath)) {
    stop("The specified file does not exist.")
  }
  
  # Extract metadata using the pdftools package
  pdfInfo <- tryCatch({
    pdftools::pdf_info(pdfPath)
  }, error = function(e) {
    warning("Failed to extract PDF metadata: ", e$message)
    list(
      title = NA,
      author = NA,
      keywords = NA,
      creation_date = NA,
      modification_date = NA
    )
  })
  
  # Extract text from all pages using the pdftools package
  pdfTextPages <- pdftools::pdf_text(pdfPath)
  
  # Function to clean text by removing unwanted spaces within words
  cleanText <- function(text) {
    # Remove unwanted spaces within words
    text <-
      gsub("\\b(\\w)\\s+(\\w)\\b", "\\1\\2", text, perl = TRUE)
    # Replace newline characters with a space if they are not followed by a capital letter (to handle wrapped lines)
    text <-
      gsub("(?<!\\.)\\s*\\n\\s*(?![A-Z])", " ", text, perl = TRUE)
    # Normalize multiple spaces
    text <- gsub("\\s{2,}", " ", text)
    return(text)
  }
  
  # Normalize text and parse into paragraphs for each page
  paragraphsAllPages <- lapply(pdfTextPages, function(pageText) {
    # Clean the text
    pageText <- cleanText(pageText)
    # Split paragraphs by periods followed by spaces and a capital letter
    paragraphs <-
      unlist(strsplit(pageText, "(?<=\\.)\\s+(?=[A-Z])", perl = TRUE))
    # Trim leading and trailing whitespace
    paragraphs <- lapply(paragraphs, trimws)
    # Filter out empty paragraphs
    paragraphs <- Filter(function(p)
      nchar(p) > 0, paragraphs)
    paragraphs
  })
  
  # Flatten the list of paragraphs from all pages
  paragraphsFlat <- unlist(paragraphsAllPages, recursive = FALSE)
  
  # Combine metadata and paragraphs into a JSON object
  result <- list(metadata = pdfInfo, paragraphs = paragraphsFlat)
  return(jsonlite::toJSON(result, auto_unbox = TRUE, pretty = TRUE))
}
