#' Retrieve PMC XML
#'
#' This function fetches the full text of an article in XML format from PubMed Central (PMC) using a given PMC ID.
#' If the direct retrieval fails, it searches for an open access article via europepmc and attempts to download it.
#'
#' @param pmcId A character string representing the PMC ID of the article.
#'
#' @return A character string containing the full text of the article in XML format, or an error message if unable to retrieve.
#' @export
retrievePMCXMLUsingTidyPmc <- function(pmcId) {
  # Attempt to fetch the full text in XML format using tidypmc
  articleXml <- tryCatch({
    tidypmc::pmc_xml(pmcId)
  }, error = function(e) {
    # On failure, search for the article using europepmc and try to download it
    message("Failed to retrieve PMC XML: ", e$message)
    message("Attempting to retrieve via europepmc package...")
    searchResults <-
      europepmc::epmc_search(query = paste0("PMC", pmcId, " OPEN_ACCESS:Y"),
                             limit = 1)
    if (nrow(searchResults) > 0 &&
        "pmcid" %in% names(searchResults)) {
      tidypmc::pmc_xml(searchResults$pmcid[1])
    } else {
      message("No open access records found for PMC ID: ", pmcId)
      return(NULL)
    }
  })
  
  return(articleXml)
}
