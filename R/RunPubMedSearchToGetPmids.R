# Define a function to perform a PubMed search and retrieve all results
#' @export
runPubMedSearchToGetPmids <-
  function(searchString) {
    # Initial search to get WebEnv and QueryKey
    initialSearch <-
      rentrez::entrez_search(db = "pubmed",
                             term = searchString,
                             use_history = TRUE)
    
    # Total number of hits
    totalCount <- initialSearch$count
    
    # Batch size
    batchSize <- 200
    
    # Retrieve all PubMed IDs
    allPmids <-
      vector("list", length = ceiling(totalCount / batchSize))
    for (i in seq_len(length(allPmids))) {
      allPmids[[i]] <-
        rentrez::entrez_search(
          db = "pubmed",
          term = searchString,
          WebEnv = initialSearch$WebEnv,
          query_key = initialSearch$query_key,
          retmax = batchSize,
          retstart = (i - 1) * batchSize
        )$ids
    }
    # Flatten the list of PMIDs
    allPmids <- unlist(allPmids)
    return(allPmids)
  }