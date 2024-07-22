#' #' Query PubMed Database
#' #'
#' #' This function queries the PubMed database using specified search criteria. It supports
#' #' a default search on various epidemiological and cohort terms and can be extended
#' #' to include specific affiliation filters such as Johnson & Johnson.
#' #' @param meshTerm Character string, the main term to include in the search.
#' #' @param affiliatedJnJ Logical, set to TRUE to include Johnson & Johnson affiliated articles,
#' #'        filtering out certain publication types (e.g., clinical trials).
#' #' @return An XML document.
#' #' @export
#' getPubMedQueryResults <- function(meshTerm, affiliatedJnJ = FALSE) {
#'   completeQuery <-
#'     getPubmQuerySearchString(meshTerm = meshTerm, affiliatedJnJ = affiliatedJnJ)
#'   
#'   # Perform the search
#'   searchResults <-
#'     rentrez::entrez_search(db = "pubmed",
#'                            term = completeQuery,
#'                            use_history = TRUE)
#'   
#'   # Fetch detailed information if results exist
#'   if (searchResults$count > 0) {
#'     fetchedDetails <- rentrez::entrez_fetch(
#'       db = "pubmed",
#'       id = searchResults$ids,
#'       rettype = "xml",
#'       # Use XML to fetch detailed fields
#'       parsed = TRUE
#'     )
#'     return(fetchedDetails)
#'   }
#' }