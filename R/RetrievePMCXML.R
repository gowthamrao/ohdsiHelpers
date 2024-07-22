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

#' Retrieve and Save PMC XML Using Multiple Methods
#'
#' This function attempts to retrieve XML data for a list of PMC IDs using the tidy PMC method first.
#' If the tidy PMC method fails, it then tries using the Rentrez method. It saves the retrieved XML
#' to files in directories corresponding to the retrieval method used within a specified output folder.
#'
#' @param pmcIds A vector of PMC IDs for which to retrieve XML data.
#' @param outputFolder A character string specifying the output folder location. The default is
#' the current working directory.
#'
#' @return A list with two elements:
#' \itemize{
#'   \item{success}{A list of successfully retrieved and saved XML contents, indexed by PMC ID.}
#'   \item{failed}{A list of PMC IDs for which XML retrieval failed using both methods, set to \code{NA}.}
#' }
#'
#' @export
retrieveAndSavePmcXmlForPmcIds <-
  function(pmcIds, outputFolder = ".") {
    xmlList <- list()
    failures <- list()
    
    for (pmcId in pmcIds) {
      xmlContent <-
        OhdsiHelpers::retrievePMCXMLUsingTidyPmc(pmcId = pmcId)
      methodUsed <- "tidyPmc"
      outputDir <- file.path(outputFolder, methodUsed)
      
      if (is.null(xmlContent)) {
        writeLines(paste0(
          "Failed to retrieve XML for ",
          pmcId,
          " using tidy PMC, trying Rentrez."
        ))
        xmlContent <- retrievePMCXMLUsingRentrez(pmcId = pmcId)
        methodUsed <- "rentrez"
        outputDir <- file.path(outputFolder, methodUsed)
      }
      
      outputFile <- file.path(outputDir, paste0(pmcId, ".xml"))
      dir.create(outputDir, recursive = TRUE, showWarnings = FALSE)
      
      if (!is.null(xmlContent)) {
        if (methodUsed == "tidyPmc") {
          xml2::write_xml(x = xmlContent, file = outputFile)
        } else {
          XML::saveXML(doc = xmlContent, file = outputFile)
        }
        xmlList[[pmcId]] <- xmlContent
        writeLines(paste0("Saved XML for ", pmcId, " to ", outputFile))
      } else {
        writeLines(paste0("Failed to retrieve XML for ", pmcId, " using both methods."))
        failures[[pmcId]] <- NA
      }
    }
    
    list(success = xmlList, failed = failures)
  }

#' @export
extractXmlSections <- function(xmlFilePath, sectionKeywords) {
  # Read the XML file
  doc <- xml2::read_xml(xmlFilePath)
  
  # Initialize a list to store extracted sections
  extractedSections <- list()
  
  # Iterate through section keywords and their synonyms
  for (keyword in names(sectionKeywords)) {
    # Find nodes that match the keyword or any of its synonyms
    nodes <- xml2::xml_find_all(doc, paste0(".//", sectionKeywords[[keyword]], collapse = " | "))
    
    if (length(nodes) > 0) {
      # Extract text content from the nodes, combining multiple nodes if needed
      sectionText <- paste(trimws(xml_text(nodes)), collapse = "\n") 
      extractedSections[[keyword]] <- sectionText
    }
  }
  
  # Handle tables (adjust if table structure is different)
  tables <- xml_find_all(doc, ".//table-wrap")
  if (length(tables) > 0) {
    extractedSections[["tables"]] <- tables
  }
  
  return(extractedSections)
}
