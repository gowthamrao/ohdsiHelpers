#' Query PubMed Database
#'
#' This function queries the PubMed database using specified search criteria. It supports
#' a default search on various epidemiological and cohort terms and can be extended
#' to include specific affiliation filters such as Johnson & Johnson.
#' @param searchText Character string, the main term to include in the search.
#' @param affiliatedJnJ Logical, set to TRUE to include Johnson & Johnson affiliated articles,
#'        filtering out certain publication types (e.g., clinical trials).
#' @return An XML document.
#' @export
pubMedQuery <- function(searchText, affiliatedJnJ = FALSE) {
  # Check input types
  checkmate::assertString(searchText, null.ok = FALSE)
  checkmate::assertLogical(affiliatedJnJ, len = 1)
  
  # Define the fixed query parts
  baseQuery <- paste0(
    "(",
    "(",
    "(retrospective cohort) OR (epidemiology[MeSH Terms]) OR (Epidemiologic Methods[MeSH Terms])",
    " OR (phenotype[Text Word]) OR (Validation Study[Publication Type])",
    " OR (positive predictive value[Text Word]) OR (Validation Studies as Topic[MeSH Terms])",
    " OR (Sensitivity and Specificity[MeSH Terms])",
    " OR (insurance OR claims OR administrative OR health care)",
    ") OR database OR algorithm",
    ") AND (",
    "(",
    "(Medicaid) OR (Medicare) OR (Truven) OR (Optum) OR (Medstat)",
    " OR (Nationwide Inpatient Sample) OR (National Inpatient Sample) OR (PharMetrics)",
    " OR (PHARMO) OR (ICD-9[Title/Abstract]) OR (ICD-10[Title/Abstract]) OR (IMS[Title/Abstract])",
    " OR (electronic medical record[Text Word]) OR (Denmark/epidemiology[MeSH Terms])",
    " OR (Veterans Affairs[Title/Abstract]) OR (Premier database[Title/Abstract])",
    " OR (Database Management System[MeSH Terms])",
    " OR (National Health Insurance Research [MeSH Terms])",
    " OR (administrative claims[Text Word]) OR (General Practice Research Database[Text Word])",
    " OR (Clinical Practice Research Datalink[Text Word])",
    " OR (The Health Improvement Network[Text Word])",
    ")",
    ")"
  )
  
  # Add affiliated J&J query if specified
  if (affiliatedJnJ) {
    affiliated <- paste0(
      "AND (",
      "\"Johnson & Johnson\"[Affiliation] OR \"Johnson and Johnson\"[Affiliation]",
      " OR \"Janssen\"[Affiliation] OR \"Actelion\"[Affiliation] OR \"Cilag\"[Affiliation]",
      " OR \"Ortho-McNeil\"[Affiliation] OR \"Ethicon\"[Affiliation] OR \"DePuy\"[Affiliation]",
      " OR \"Tibotec\"[Affiliation]",
      ") NOT (",
      "(",
      "\"Clinical Trial\"[pt] OR \"Editorial\"[pt] OR \"Letter\"[pt]",
      " OR \"Randomized Controlled Trial\"[pt] OR \"Clinical Trial, Phase I\"[pt]",
      " OR \"Clinical Trial, Phase II\"[pt] OR \"Clinical Trial, Phase III\"[pt]",
      " OR \"Clinical Trial, Phase IV\"[pt] OR \"Comment\"[pt] OR \"Controlled Clinical Trial\"[pt]",
      " OR \"Case Reports\"[pt] OR \"Clinical Trials as Topic\"[Mesh] OR \"double-blind\"[All]",
      " OR \"placebo-controlled\"[All] OR \"pilot study\"[All] OR \"pilot projects\"[Mesh]",
      " OR \"Prospective Studies\"[Mesh] OR \"Genetics\"[Mesh] OR \"Genotype\"[Mesh]",
      " OR (biomarker[Title/Abstract])",
      ")",
      ")"
    )
    baseQuery <- paste0(baseQuery, affiliated)
  }
  
  # Combine user input with fixed query
  completeQuery <- paste(searchText, baseQuery, sep = " AND ")
  
  # Perform the search
  searchResults <-
    rentrez::entrez_search(db = "pubmed", term = completeQuery)
  
  # Fetch detailed information if results exist
  if (searchResults$count > 0) {
    fetchedDetails <- rentrez::entrez_fetch(
      db = "pubmed",
      id = searchResults$ids,
      rettype = "xml",
      # Use XML to fetch detailed fields
      parsed = TRUE
    )
    return(fetchedDetails)
  } else {
    return("No results found.")
  }
}