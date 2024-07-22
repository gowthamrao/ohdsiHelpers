#' #' Parse XML Data from PubMed
#' #'
#' #' This function parses XML data obtained from PubMed to extract detailed information
#' #' about articles. The expected XML format should include specific nodes such as
#' #' PubmedArticle, MedlineCitation, Article, Journal, and others. It retrieves various
#' #' details such as article titles, publication dates, author information, and more.
#' #'
#' #' @param xmlData An XML document containing PubMed article data. The XML document
#' #' should have a structure similar to the one provided by PubMed.
#' #' @return A tibble with columns for various details of each article. The tibble includes,
#' #' but is not limited to, the following columns:
#' #' \item{pubmedArticle}{The entire PubmedArticle node content.}
#' #' \item{medlineCitationStatus}{The status of the MedlineCitation.}
#' #' \item{medlineCitationOwner}{The owner of the MedlineCitation.}
#' #' \item{indexingMethod}{The indexing method of the MedlineCitation.}
#' #' \item{pmid}{The PubMed ID of the article.}
#' #' \item{pmcid}{The PubMed Central ID of the article.}
#' #' \item{dateRevisedYear}{The year the article was revised.}
#' #' \item{dateRevisedMonth}{The month the article was revised.}
#' #' \item{dateRevisedDay}{The day the article was revised.}
#' #' \item{articlePubModel}{The publication model of the article.}
#' #' \item{journalIssn}{The ISSN of the journal.}
#' #' \item{journalIssnType}{The type of ISSN of the journal.}
#' #' \item{journalIssueVolume}{The volume number of the journal issue.}
#' #' \item{journalIssueIssue}{The issue number of the journal issue.}
#' #' \item{journalIssueCitedMedium}{The cited medium of the journal issue.}
#' #' \item{journalIssuePubDateYear}{The year of publication of the journal issue.}
#' #' \item{journalIssuePubDateMonth}{The month of publication of the journal issue.}
#' #' \item{journalTitle}{The title of the journal.}
#' #' \item{journalISOAbbreviation}{The ISO abbreviation of the journal.}
#' #' \item{articleTitle}{The title of the article.}
#' #' \item{paginationStartPage}{The starting page number of the article.}
#' #' \item{paginationEndPage}{The ending page number of the article.}
#' #' \item{paginationMedlinePgn}{The Medline page number of the article.}
#' #' \item{eLocationId}{The electronic location ID of the article.}
#' #' \item{eLocationIdType}{The type of electronic location ID of the article.}
#' #' \item{eLocationIdValidYN}{The validity of the electronic location ID of the article.}
#' #' \item{abstractText}{The abstract text of the article.}
#' #' \item{abstractCopyrightInformation}{The copyright information of the abstract.}
#' #' \item{authorListCompleteYN}{Indicates if the author list is complete.}
#' #' \item{authorValidYN}{Validity of each author.}
#' #' \item{authorLastName}{The last names of the authors.}
#' #' \item{authorForeName}{The fore names of the authors.}
#' #' \item{authorInitials}{The initials of the authors.}
#' #' \item{authorAffiliation}{The affiliations of the authors.}
#' #' \item{language}{The language of the article.}
#' #' \item{publicationType}{The type of publication.}
#' #' \item{publicationTypeUI}{The UI of the publication type.}
#' #' \item{articleDateType}{The type of article date.}
#' #' \item{articleDateYear}{The year of the article date.}
#' #' \item{articleDateMonth}{The month of the article date.}
#' #' \item{articleDateDay}{The day of the article date.}
#' #' \item{medlineJournalInfoCountry}{The country of the Medline journal.}
#' #' \item{medlineJournalInfoMedlineTA}{The Medline TA of the journal.}
#' #' \item{medlineJournalInfoNlmUniqueID}{The NLM unique ID of the journal.}
#' #' \item{medlineJournalInfoISSNLinking}{The ISSN linking of the journal.}
#' #' \item{coiStatement}{The conflict of interest statement of the article.}
#' #' \item{pubmedDataHistoryPubStatus}{The publication status history.}
#' #' \item{pubmedDataHistoryPubDateYear}{The publication date years.}
#' #' \item{pubmedDataHistoryPubDateMonth}{The publication date months.}
#' #' \item{pubmedDataHistoryPubDateDay}{The publication date days.}
#' #' \item{pubmedDataHistoryPubDateHour}{The publication date hours.}
#' #' \item{pubmedDataHistoryPubDateMinute}{The publication date minutes.}
#' #' \item{publicationStatus}{The publication status.}
#' #' \item{articleIdType}{The types of article IDs.}
#' #' \item{articleId}{The article IDs.}
#' #' \item{referenceCitation}{The reference citations.}
#' #' @export
#' parsePubMedXML <- function(xmlData) {
#'   # Check if the input is an XML document
#'   if (!inherits(xmlData, c("XMLInternalDocument", "XMLAbstractDocument"))) {
#'     stop("Input must be an XML document.")
#'   }
#'   
#'   # Get the root node
#'   root <- XML::xmlRoot(xmlData)
#'   
#'   # Extract relevant nodes
#'   articles <- XML::getNodeSet(root, "//PubmedArticle")
#'   
#'   # Function to safely extract XML values with error handling
#'   safeExtract <- function(nodeSet, xpath, attr = NULL) {
#'     tryCatch({
#'       if (!is.null(attr)) {
#'         XML::xmlGetAttr(XML::getNodeSet(nodeSet, xpath)[[1]], attr)
#'       } else {
#'         XML::xmlValue(XML::getNodeSet(nodeSet, xpath)[[1]])
#'       }
#'     }, error = function(e)
#'       NA)
#'   }
#'   
#'   # Map over articles to extract details
#'   articleDetails <- tibble::tibble(
#'     pubmedArticle = sapply(articles, function(article)
#'       safeExtract(article, ".")),
#'     medlineCitationStatus = sapply(articles, function(article)
#'       safeExtract(article, ".//MedlineCitation", "Status")),
#'     medlineCitationOwner = sapply(articles, function(article)
#'       safeExtract(article, ".//MedlineCitation", "Owner")),
#'     indexingMethod = sapply(articles, function(article)
#'       safeExtract(article, ".//MedlineCitation", "IndexingMethod")),
#'     pmid = sapply(articles, function(article)
#'       safeExtract(article, ".//PMID")),
#'     pmcid = sapply(articles, function(article)
#'       safeExtract(
#'         article,
#'         ".//PubmedData/ArticleIdList/ArticleId[@IdType='pmc']"
#'       )),
#'     dateRevisedYear = sapply(articles, function(article)
#'       safeExtract(article, ".//DateRevised/Year")),
#'     dateRevisedMonth = sapply(articles, function(article)
#'       safeExtract(article, ".//DateRevised/Month")),
#'     dateRevisedDay = sapply(articles, function(article)
#'       safeExtract(article, ".//DateRevised/Day")),
#'     articlePubModel = sapply(articles, function(article)
#'       safeExtract(article, ".//Article", "PubModel")),
#'     journalIssn = sapply(articles, function(article)
#'       safeExtract(article, ".//Journal/ISSN")),
#'     journalIssnType = sapply(articles, function(article)
#'       safeExtract(article, ".//Journal/ISSN", "IssnType")),
#'     journalIssueVolume = sapply(articles, function(article)
#'       safeExtract(article, ".//JournalIssue/Volume")),
#'     journalIssueIssue = sapply(articles, function(article)
#'       safeExtract(article, ".//JournalIssue/Issue")),
#'     journalIssueCitedMedium = sapply(articles, function(article)
#'       safeExtract(article, ".//JournalIssue", "CitedMedium")),
#'     journalIssuePubDateYear = sapply(articles, function(article)
#'       safeExtract(article, ".//JournalIssue/PubDate/Year")),
#'     journalIssuePubDateMonth = sapply(articles, function(article)
#'       safeExtract(article, ".//JournalIssue/PubDate/Month")),
#'     journalTitle = sapply(articles, function(article)
#'       safeExtract(article, ".//Journal/Title")),
#'     journalISOAbbreviation = sapply(articles, function(article)
#'       safeExtract(article, ".//Journal/ISOAbbreviation")),
#'     articleTitle = sapply(articles, function(article)
#'       safeExtract(article, ".//ArticleTitle")),
#'     paginationStartPage = sapply(articles, function(article)
#'       safeExtract(article, ".//Pagination/StartPage")),
#'     paginationEndPage = sapply(articles, function(article)
#'       safeExtract(article, ".//Pagination/EndPage")),
#'     paginationMedlinePgn = sapply(articles, function(article)
#'       safeExtract(article, ".//Pagination/MedlinePgn")),
#'     eLocationId = sapply(articles, function(article)
#'       safeExtract(article, ".//ELocationID")),
#'     eLocationIdType = sapply(articles, function(article)
#'       safeExtract(article, ".//ELocationID", "EIdType")),
#'     eLocationIdValidYN = sapply(articles, function(article)
#'       safeExtract(article, ".//ELocationID", "ValidYN")),
#'     abstractText = sapply(articles, function(article)
#'       safeExtract(article, ".//Abstract/AbstractText")),
#'     abstractCopyrightInformation = sapply(articles, function(article)
#'       safeExtract(article, ".//Abstract/CopyrightInformation")),
#'     authorListCompleteYN = sapply(articles, function(article)
#'       safeExtract(article, ".//AuthorList", "CompleteYN")),
#'     authorValidYN = sapply(articles, function(article)
#'       paste(
#'         sapply(XML::getNodeSet(article, ".//Author"), function(author)
#'           safeExtract(author, ".", "ValidYN")), collapse = "; "
#'       )),
#'     authorLastName = sapply(articles, function(article)
#'       paste(
#'         sapply(XML::getNodeSet(article, ".//Author"), function(author)
#'           safeExtract(author, ".//LastName")), collapse = "; "
#'       )),
#'     authorForeName = sapply(articles, function(article)
#'       paste(
#'         sapply(XML::getNodeSet(article, ".//Author"), function(author)
#'           safeExtract(author, ".//ForeName")), collapse = "; "
#'       )),
#'     authorInitials = sapply(articles, function(article)
#'       paste(
#'         sapply(XML::getNodeSet(article, ".//Author"), function(author)
#'           safeExtract(author, ".//Initials")), collapse = "; "
#'       )),
#'     authorAffiliation = sapply(articles, function(article)
#'       paste(
#'         sapply(XML::getNodeSet(article, ".//Author/AffiliationInfo/Affiliation"), function(affiliation)
#'           safeExtract(affiliation, ".")), collapse = "; "
#'       )),
#'     language = sapply(articles, function(article)
#'       safeExtract(article, ".//Language")),
#'     publicationType = sapply(articles, function(article)
#'       paste(
#'         sapply(XML::getNodeSet(article, ".//PublicationTypeList/PublicationType"), function(pubType)
#'           safeExtract(pubType, ".")), collapse = "; "
#'       )),
#'     publicationTypeUI = sapply(articles, function(article)
#'       paste(
#'         sapply(XML::getNodeSet(article, ".//PublicationTypeList/PublicationType"), function(pubType)
#'           safeExtract(pubType, ".", "UI")), collapse = "; "
#'       )),
#'     articleDateType = sapply(articles, function(article)
#'       safeExtract(article, ".//ArticleDate", "DateType")),
#'     articleDateYear = sapply(articles, function(article)
#'       safeExtract(article, ".//ArticleDate/Year")),
#'     articleDateMonth = sapply(articles, function(article)
#'       safeExtract(article, ".//ArticleDate/Month")),
#'     articleDateDay = sapply(articles, function(article)
#'       safeExtract(article, ".//ArticleDate/Day")),
#'     medlineJournalInfoCountry = sapply(articles, function(article)
#'       safeExtract(article, ".//MedlineJournalInfo/Country")),
#'     medlineJournalInfoMedlineTA = sapply(articles, function(article)
#'       safeExtract(article, ".//MedlineJournalInfo/MedlineTA")),
#'     medlineJournalInfoNlmUniqueID = sapply(articles, function(article)
#'       safeExtract(article, ".//MedlineJournalInfo/NlmUniqueID")),
#'     medlineJournalInfoISSNLinking = sapply(articles, function(article)
#'       safeExtract(article, ".//MedlineJournalInfo/ISSNLinking")),
#'     coiStatement = sapply(articles, function(article)
#'       safeExtract(article, ".//CoiStatement")),
#'     pubmedDataHistoryPubStatus = sapply(articles, function(article)
#'       paste(
#'         sapply(XML::getNodeSet(article, ".//PubmedData/History/PubMedPubDate"), function(pubDate)
#'           safeExtract(pubDate, ".", "PubStatus")), collapse = "; "
#'       )),
#'     pubmedDataHistoryPubDateYear = sapply(articles, function(article)
#'       paste(
#'         sapply(XML::getNodeSet(article, ".//PubmedData/History/PubMedPubDate"), function(pubDate)
#'           safeExtract(pubDate, ".//Year")), collapse = "; "
#'       )),
#'     pubmedDataHistoryPubDateMonth = sapply(articles, function(article)
#'       paste(
#'         sapply(XML::getNodeSet(article, ".//PubmedData/History/PubMedPubDate"), function(pubDate)
#'           safeExtract(pubDate, ".//Month")), collapse = "; "
#'       )),
#'     pubmedDataHistoryPubDateDay = sapply(articles, function(article)
#'       paste(
#'         sapply(XML::getNodeSet(article, ".//PubmedData/History/PubMedPubDate"), function(pubDate)
#'           safeExtract(pubDate, ".//Day")), collapse = "; "
#'       )),
#'     pubmedDataHistoryPubDateHour = sapply(articles, function(article)
#'       paste(
#'         sapply(XML::getNodeSet(article, ".//PubmedData/History/PubMedPubDate"), function(pubDate)
#'           safeExtract(pubDate, ".//Hour")), collapse = "; "
#'       )),
#'     pubmedDataHistoryPubDateMinute = sapply(articles, function(article)
#'       paste(
#'         sapply(XML::getNodeSet(article, ".//PubmedData/History/PubMedPubDate"), function(pubDate)
#'           safeExtract(pubDate, ".//Minute")), collapse = "; "
#'       )),
#'     publicationStatus = sapply(articles, function(article)
#'       safeExtract(article, ".//PubmedData/PublicationStatus")),
#'     articleIdType = sapply(articles, function(article)
#'       paste(
#'         sapply(XML::getNodeSet(article, ".//PubmedData/ArticleIdList/ArticleId"), function(articleId)
#'           safeExtract(articleId, ".", "IdType")), collapse = "; "
#'       )),
#'     articleId = sapply(articles, function(article)
#'       paste(
#'         sapply(XML::getNodeSet(article, ".//PubmedData/ArticleIdList/ArticleId"), function(articleId)
#'           safeExtract(articleId, ".")), collapse = "; "
#'       )),
#'     referenceCitation = sapply(articles, function(article)
#'       paste(
#'         sapply(XML::getNodeSet(article, ".//PubmedData/ReferenceList/Reference/Citation"), function(citation)
#'           safeExtract(citation, ".")), collapse = "; "
#'       ))
#'   )
#'   
#'   return(articleDetails)
#' }
