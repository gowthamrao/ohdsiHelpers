#' Retrieve PMCIDs for a vector of PubMed IDs (PMIDs)
#'
#' This function queries the PubMed database using the NCBI's E-utilities API to fetch the corresponding
#' PMCIDs for given PMIDs. The results are extracted from the XML response and returned in a data frame.
#' @param pmids A character vector specifying the PubMed IDs for which PMCIDs are sought.
#' @return A data frame with columns 'PMID' and 'PMCID', representing the PMCID associated with each given PMID.
#' @export
getPubMedPmcIdsFromPmIds <- function(pmids) {
  base_url <-
    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
  results <-
    data.frame(PMID = character(),
               PMCID = character(),
               stringsAsFactors = FALSE)
  
  # Process in batches to manage request sizes and API constraints
  batch_size <- 200  # Define the size of each batch
  for (i in seq(1, length(pmids), by = batch_size)) {
    batch_pmids <-
      paste(pmids[i:min(i + batch_size - 1, length(pmids))], collapse = ",")
    query_list <-
      list(db = "pubmed",
           id = batch_pmids,
           retmode = "xml")
    response <- httr::GET(url = base_url, query = query_list)
    
    # Parse XML response
    content <- httr::content(response, "text")
    xml_content <- xml2::read_xml(content)
    
    # Extract PMCIDs for all PMIDs in the batch
    articles <- xml2::xml_find_all(xml_content, "//PubmedArticle")
    for (article in articles) {
      pmid_node <-
        xml2::xml_find_first(article, ".//ArticleId[@IdType='pubmed']")
      pmcid_node <-
        xml2::xml_find_first(article, ".//ArticleId[@IdType='pmc']")
      pmid <- xml2::xml_text(pmid_node)
      pmcid <-
        if (!is.na(pmcid_node))
          xml2::xml_text(pmcid_node)
      else
        NA
      results <-
        rbind(results,
              data.frame(
                pmid = pmid,
                pmcid = pmcid,
                stringsAsFactors = FALSE
              )) |> dplyr::tibble() |>
        dplyr::arrange(pmid,
                       pmcid)
    }
  }
  return(results)
}
