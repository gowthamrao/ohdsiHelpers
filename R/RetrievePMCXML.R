#' Retrieve PMC XML
#'
#' This function fetches the full text of an article in XML format from PubMed Central (PMC) using a given PMC ID.
#'
#' @param pmcId A character string representing the PMC ID of the article.
#' 
#' @return A character string containing the full text of the article in XML format.
#' @export
retrievePMCXML <- function(pmcId) {
  # Fetch the full text in XML format
  articleXml <- rentrez::entrez_fetch(db = "pmc", id = pmcId, rettype = "xml")
  return(articleXml)
}