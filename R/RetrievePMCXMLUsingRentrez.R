#' Retrieve PMC XML
#'
#' This function fetches the full text of an article in XML format from PubMed Central (PMC) using a given PMC ID.
#' This function employs the rentrez package to fetch the XML. If an error occurs during the fetching process,
#' an appropriate error message is returned.
#'
#' @param pmcId A character string representing the PMC ID of the article.
#'
#' @return A character string containing the full text of the article in XML format, or an error message if unable to retrieve.
#' @export
retrievePMCXMLUsingRentrez <- function(pmcId) {
  # Fetch the full text in XML format
  articleXml <- tryCatch({
    rentrez::entrez_fetch(
      db = "pmc",
      id = pmcId,
      rettype = "xml",
      parsed = TRUE
    )
  }, error = function(e) {
    message("Error retrieving PMC XML: ", e$message)
    return(NULL)
  })
  
  return(articleXml)
}
