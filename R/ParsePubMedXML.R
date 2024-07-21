#' Parse XML Data from PubMed
#'
#' This function parses XML data obtained from PubMed to extract information
#' about articles. The expected XML format should include specific nodes such as
#' PubmedArticle, ArticleTitle, PubDate, Author, LastName, Initials, and PMID.
#'
#' @param xml_data XML document containing PubMed article data.
#' @return A tibble with columns for Title, Publication Date, Authors, and PMID of each article.
#' @export
parsePubMedXML <- function(xml_data) {
  # Check if the input is an XML document
  if (!inherits(xml_data, c("XMLInternalDocument", "XMLAbstractDocument"))) {
    stop("Input must be an XML document.")
  }
  
  # Get the root node
  root <- XML::xmlRoot(xml_data)
  
  # Extract relevant nodes
  articles <- XML::getNodeSet(root, "//PubmedArticle")
  
  # Map over articles to extract details
  article_details <- tibble::tibble(
    Title = sapply(articles, function(article)
      XML::xmlValue(
        XML::getNodeSet(article, ".//ArticleTitle")[[1]]
      )),
    PublicationDate = sapply(articles, function(article)
      XML::xmlValue(
        XML::getNodeSet(article, ".//PubDate/Year")[[1]]
      )),
    Authors = sapply(articles, function(article) {
      authorNodes <- XML::getNodeSet(article, ".//Author")
      authorNames <- sapply(authorNodes, function(author) {
        lastName <-
          XML::xmlValue(XML::getNodeSet(author, ".//LastName")[[1]])
        initials <-
          XML::xmlValue(XML::getNodeSet(author, ".//Initials")[[1]])
        paste(lastName, initials)
      })
      paste(authorNames, collapse = ", ")
    }),
    PMID = sapply(articles, function(article)
      XML::xmlValue(XML::getNodeSet(
        article, ".//PMID"
      )[[1]]))
  )
  
  return(article_details)
}
