#' Retrieve PubMed Article Summaries for a Set of PMIDs
#'
#' This function uses the NCBI E-utilities API to fetch detailed summaries for a list of PubMed IDs (PMIDs).
#' It handles multiple PMIDs by processing them in batches to stay within the API's rate limits.
#' The results include detailed publication data formatted in a tibble.
#' @param pmids A vector of character strings representing the PubMed IDs for which summaries are required.
#' @return A tibble containing detailed article summaries including publication URLs, PMC IDs, abstracts, and other metadata.
#' @export
getPubMedArticleSummaries <- function(pmids) {
  library(rentrez)
  library(dplyr)
  library(tibble)
  library(purrr)
  library(xml2)
  
  # Check for empty PMIDs input
  if (length(pmids) == 0) {
    return(tibble(pmid = character()))  # Returns an empty tibble if no PMIDs are provided
  }
  
  # Initialize tibble to accumulate results
  results <- tibble()
  
  # Process in batches to avoid API limit
  batch_size <-
    50  # Define batch size (adjust based on your rate limit)
  pmid_batches <-
    split(pmids, ceiling(seq_along(pmids) / batch_size))
  
  for (pmids in pmid_batches) {
    # Attempt to retrieve summaries, retrying if rate limit exceeded
    repeat {
      tryCatch({
        articleSummaries <-
          rentrez::entrez_summary(db = "pubmed", id = pmids)
        break  # Exit the repeat loop on success
      }, error = function(e) {
        if (grepl("API rate limit exceeded", e$message)) {
          Sys.sleep(60)  # Wait 60 seconds before retrying
        } else {
          stop(e)  # Re-throw the error if it's not a rate limit issue
        }
      })
    }
    
    summariesList <- setNames(as.list(articleSummaries), pmids)
    
    # Construct the tibble from the list of articles
    articlesData <- map_df(names(summariesList), function(pmid) {
      article <- summariesList[[pmid]]
      
      # Fetch the full record for the given PMID to get additional details
      full_record_xml <-
        rentrez::entrez_fetch(
          db = "pubmed",
          id = pmid,
          rettype = "xml",
          parsed = FALSE
        )
      full_record <- xml2::read_xml(full_record_xml)
      
      abstract <- xml2::xml_find_all(full_record, ".//AbstractText")
      abstract_text <- xml2::xml_text(abstract)
      abstract_combined <- paste(abstract_text, collapse = " ")
      
      doi <-
        xml2::xml_text(xml2::xml_find_first(full_record, ".//ELocationID[@EIdType='doi']"))
      journal <-
        xml2::xml_text(xml2::xml_find_first(full_record, ".//Journal/Title"))
      keywords <-
        xml2::xml_text(xml2::xml_find_all(full_record, ".//Keyword"))
      keywords_combined <- paste(keywords, collapse = ", ")
      article_ids <-
        xml2::xml_text(xml2::xml_find_all(full_record, ".//ArticleIdList/ArticleId"))
      article_ids_combined <- paste(article_ids, collapse = ", ")
      pub_type <-
        xml2::xml_text(xml2::xml_find_all(full_record, ".//PublicationType"))
      pub_type_combined <- paste(pub_type, collapse = ", ")
      title <-
        xml2::xml_text(xml2::xml_find_first(full_record, ".//ArticleTitle"))
      authors <-
        xml2::xml_text(xml2::xml_find_all(full_record, ".//AuthorList/Author/LastName"))
      authors_combined <- paste(authors, collapse = ", ")
      language <-
        xml2::xml_text(xml2::xml_find_first(full_record, ".//Language"))
      pub_date <-
        xml2::xml_text(xml2::xml_find_first(full_record, ".//PubDate"))
      affiliations <-
        xml2::xml_text(xml2::xml_find_all(full_record, ".//AffiliationInfo/Affiliation"))
      affiliations_combined <- paste(affiliations, collapse = "; ")
      grants <-
        xml2::xml_text(xml2::xml_find_all(full_record, ".//GrantList/Grant"))
      grants_combined <- paste(grants, collapse = "; ")
      
      if (is.null(article)) {
        return(tibble(pmid = pmid, uid = NA_character_))  # Return a row with NA values if no summary data
      } else {
        tibble(
          pmid = pmid,
          pubmedUrl = paste0("https://pubmed.ncbi.nlm.nih.gov/", pmid, "/"),
          uid = safeExtract(article, "uid"),
          pubdate = safeExtract(article, "pubdate"),
          epubdate = safeExtract(article, "epubdate"),
          source = safeExtract(article, "source"),
          authors = if (!is.null(article$authors))
            toString(sapply(article$authors, `[`, "name"))
          else
            NA_character_,
          lastauthor = safeExtract(article, "lastauthor"),
          title = safeExtract(article, "title"),
          volume = safeExtract(article, "volume"),
          issue = safeExtract(article, "issue"),
          pages = safeExtract(article, "pages"),
          lang = if (!is.null(article$lang))
            safeExtract(article, "lang[1]")
          else
            NA_character_,
          issn = safeExtract(article, "issn"),
          essn = safeExtract(article, "essn"),
          pubtype = if (!is.null(article$pubtype))
            toString(article$pubtype)
          else
            NA_character_,
          recordstatus = safeExtract(article, "recordstatus"),
          pubstatus = safeExtract(article, "pubstatus"),
          pmcrefcount = if (!is.null(article$pmcrefcount))
            as.integer(article$pmcrefcount)
          else
            NA_integer_,
          fulljournalname = safeExtract(article, "fulljournalname"),
          doctype = safeExtract(article, "doctype"),
          sortpubdate = if (!is.null(article$sortpubdate))
            as.Date(article$sortpubdate)
          else
            NA,
          sortfirstauthor = safeExtract(article, "sortfirstauthor"),
          abstract = abstract_combined,
          doi = doi,
          journal = journal,
          keywords = keywords_combined,
          articleIds = article_ids_combined,
          publicationType = pub_type_combined,
          authors_full = authors_combined,
          language = language,
          publicationDate = pub_date,
          affiliations = affiliations_combined,
          grants = grants_combined
        )
      }
    })
    
    # Append batch results
    results <- bind_rows(results, articlesData)
  }
  
  return(results)
}

# Helper function to safely extract information from each article
safeExtract <- function(article, field) {
  if (!is.null(article) && !is.null(article[[field]])) {
    return(as.character(article[[field]]))
  } else {
    return(NA_character_)  # Returns NA for missing data
  }
}
