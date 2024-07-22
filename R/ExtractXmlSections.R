#' @export
extractXmlSections <- function(xmlFilePath, sectionKeywords) {
  # Read the XML file
  doc <- xml2::read_xml(xmlFilePath)
  
  # Initialize a list to store extracted sections
  extractedSections <- list()
  
  # Iterate through section keywords and their synonyms
  for (keyword in names(sectionKeywords)) {
    # Find nodes that match the keyword or any of its synonyms
    nodes <-
      xml2::xml_find_all(doc, paste0(".//", sectionKeywords[[keyword]], collapse = " | "))
    
    if (length(nodes) > 0) {
      # Extract text content from the nodes, combining multiple nodes if needed
      sectionText <-
        paste(trimws(xml_text(nodes)), collapse = "\n")
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
