#' Extract Sections from PMC XML
#'
#' This function parses an XML document from PubMed Central (PMC) and extracts text content
#' from predefined sections such as Abstract, Introduction, Methods, Results, and Discussion.
#' It also handles extraction of tables with captions. The function is case-insensitive to
#' section titles.
#'
#' @param pmcXml A character string containing the XML data or path to the XML file.
#'
#' @return A list containing the texts of the Abstract, Introduction, Methods, Results,
#'         Discussion sections, and tables with their respective captions and data.
#'
#' @import XML
#' @importFrom stringr str_squish
#' @export
extractSectionsFromPmcXml <- function(pmcXml) {
  # Load XML (assuming the XML is stored in a variable named 'pmcXml')
  doc <- XML::xmlParse(pmcXml)
  
  # Helper function to create case-insensitive XPath for equality check
  toLowerXPath <- function(section) {
    paste0(
      "translate(title, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz') = '",
      tolower(section),
      "'"
    )
  }
  
  # Extract sections with specific title and use fallback if not found
  abstract <- XML::xpathSApply(doc,
                               paste0(
                                 "//abstract/p | //abstract/sec[",
                                 toLowerXPath("summary"),
                                 "]/p"
                               ),
                               XML::xmlValue)
  introduction <- XML::xpathSApply(
    doc,
    paste0(
      "//body/sec[",
      toLowerXPath("introduction"),
      " or ",
      toLowerXPath("background"),
      "]/p"
    ),
    XML::xmlValue
  )
  
  # Fallback to the first <sec> if introduction is NULL or empty
  if (length(introduction) == 0) {
    introduction <-
      XML::xpathSApply(doc, "(//body/sec)[1]/p", XML::xmlValue)
  }
  
  methods <- XML::xpathSApply(
    doc,
    paste0(
      "//body/sec[",
      toLowerXPath("methods"),
      " or ",
      toLowerXPath("material and methods"),
      "]/p"
    ),
    XML::xmlValue
  )
  results <- XML::xpathSApply(
    doc,
    paste0(
      "//body/sec[",
      toLowerXPath("results"),
      " or ",
      toLowerXPath("findings"),
      "]/p"
    ),
    XML::xmlValue
  )
  discussion <- XML::xpathSApply(
    doc,
    paste0(
      "//body/sec[",
      toLowerXPath("discussion"),
      " or ",
      toLowerXPath("conclusion"),
      "]/p"
    ),
    XML::xmlValue
  )
  
  # Table Extraction
  tables <- list()
  tableNodes <- XML::getNodeSet(doc, "//table-wrap")
  for (i in seq_along(tableNodes)) {
    table <- tableNodes[[i]]
    caption <- XML::xpathSApply(table, ".//caption", XML::xmlValue)
    table_data <-
      XML::readHTMLTable(table, stringsAsFactors = FALSE)
    tables[[i]] <- list(caption = caption, data = table_data)
  }
  
  # Combine paragraphs within each section (adjust as needed for formatting)
  abstractText <- paste(abstract, collapse = " ")
  introductionText <- paste(introduction, collapse = " ")
  methodsText <- paste(methods, collapse = " ")
  resultsText <- paste(results, collapse = " ")
  discussionText <- paste(discussion, collapse = " ")
  
  # Remove extra whitespace and newlines
  abstractText <- stringr::str_squish(abstractText)
  introductionText <- stringr::str_squish(introductionText)
  methodsText <- stringr::str_squish(methodsText)
  resultsText <- stringr::str_squish(resultsText)
  discussionText <- stringr::str_squish(discussionText)
  
  # Return the extracted and cleaned sections as a list
  return(
    list(
      abstract = abstractText,
      introduction = introductionText,
      methods = methodsText,
      results = resultsText,
      discussion = discussionText,
      tables = tables
    )
  )
}
