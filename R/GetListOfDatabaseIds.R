#' @export
getListOfDatabaseIds <- function(databaseIds = c(
  "merative_ccae",
  "merative_mdcd",
  # 'cprd',
  "jmdc",
  "optum_extended_dod",
  "optum_extended_ses",
  "optum_ehr",
  "merative_mdcr",
  "ims_australia_lpd",
  "ims_germany",
  "ims_france",
  "health_verity",
  # 'iqvia_amb_emr',
  "iqvia_pharmetrics_plus"
  # ,
  # 'premier'
),
filterToUsData = FALSE,
filterToClaims = FALSE) {
  allDatabaseIds <- c(
    "merative_ccae",
    "merative_mdcd",
    "cprd",
    "jmdc",
    "optum_extended_dod",
    "optum_extended_ses",
    "optum_ehr",
    "merative_mdcr",
    "ims_australia_lpd",
    "ims_germany",
    "ims_france",
    "health_verity",
    "iqvia_amb_emr",
    "iqvia_pharmetrics_plus",
    "premier"
  )
  
  usDataSources <- c(
    "merative_ccae",
    "merative_mdcd",
    "optum_extended_dod",
    "optum_extended_ses",
    "optum_ehr",
    "merative_mdcr",
    "iqvia_amb_emr",
    "health_verity",
    "iqvia_pharmetrics_plus",
    "premier"
  )
  
  claimsDataSource <- c(
    "merative_ccae",
    "merative_mdcd",
    "jmdc",
    "optum_extended_dod",
    "optum_extended_ses",
    "health_verity",
    "merative_mdcr",
    "iqvia_pharmetrics_plus"
  )
  
  finalDatasources <- allDatabaseIds
  
  if (filterToUsData) {
    finalDatasources <- intersect(finalDatasources, usDataSources)
  }
  
  if (filterToClaims) {
    finalDatasources <- intersect(finalDatasources, claimsDataSource)
  }
  
  if (all(!is.null(databaseIds), length(databaseIds) > 0)) {
    finalDatasources <- intersect(finalDatasources, databaseIds)
  }
  return(finalDatasources)
}
