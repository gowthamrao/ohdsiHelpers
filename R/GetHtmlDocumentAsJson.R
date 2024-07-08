#' Extract HTML document content and metadata as a JSON object.
#'
#' This function extracts both metadata and text content from an HTML document,
#' organizes the text into structured paragraphs, and merges text blocks to improve coherence.
#' It returns the data as a structured JSON object.
#'
#' @param htmlPath The file path to the local HTML document or URL of the remote website.
#'
#' @return A JSON object containing the HTML metadata and structured text.
#' @export
getHtmlDocumentAsJson <- function(htmlPath) {
  # Load necessary libraries
  require(xml2)
  require(rvest)
  require(jsonlite)
  
  # Determine if the input is a URL or a local file
  if (grepl("^http[s]?://", htmlPath)) {
    html <- tryCatch({
      xml2::read_html(htmlPath)
    }, error = function(e) {
      stop("Failed to fetch the HTML document from the URL: ", e$message)
    })
  } else if (file.exists(htmlPath)) {
    html <- tryCatch({
      xml2::read_html(htmlPath)
    }, error = function(e) {
      stop("Failed to read the local HTML file: ", e$message)
    })
  } else {
    stop("The specified file or URL does not exist.")
  }
  
  # Extract metadata
  title <- html |> html_node("title") |> html_text(trim = TRUE)
  description <-
    html |> html_node("meta[name='description']") |> html_attr("content")
  metaTags <- html |> html_nodes("meta")
  
  # Normalize metaTags into a named list
  metaList <- list()
  for (tag in metaTags) {
    name <- tag |> html_attr("name")
    content <- tag |> html_attr("content")
    if (!is.null(name)) {
      metaList[[name]] <- content
    }
  }
  
  metadata <- list(title = title,
                   description = description,
                   metaTags = metaList)
  
  # Extract and normalize text content
  paragraphsAllPages <- html |>
    html_nodes("p") |>
    html_text(trim = TRUE)
  
  # Combine metadata and paragraphs into a JSON object
  result <-
    list(metadata = metadata, paragraphs = paragraphsAllPages)
  return(jsonlite::toJSON(result, auto_unbox = TRUE))
}
