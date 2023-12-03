#' @export
getListOfDatabaseIds <- function(
    databasIds = c(
      "truven_ccae",
      "truven_mdcd",
      # 'cprd',
      "jmdc",
      "optum_extended_dod",
      "optum_ehr",
      "truven_mdcr",
      "ims_australia_lpd",
      "ims_germany",
      "ims_france",
      # 'iqvia_amb_emr',
      "iqvia_pharmetrics_plus"
      # ,
      # 'premier'
    ),
    filterToUsData = FALSE,
    filterToClaims = FALSE) {
  allDatabasIds <- c(
    "truven_ccae",
    "truven_mdcd",
    "cprd",
    "jmdc",
    "optum_extended_dod",
    "optum_ehr",
    "truven_mdcr",
    "ims_australia_lpd",
    "ims_germany",
    "ims_france",
    "iqvia_amb_emr",
    "iqvia_pharmetrics_plus",
    "premier"
  )

  usDataSources <- c(
    "truven_ccae",
    "truven_mdcd",
    "optum_extended_dod",
    "optum_ehr",
    "truven_mdcr",
    "iqvia_amb_emr",
    "iqvia_pharmetrics_plus",
    "premier"
  )

  claimsDataSource <- c(
    "truven_ccae",
    "truven_mdcd",
    "jmdc",
    "optum_extended_dod",
    "truven_mdcr",
    "iqvia_pharmetrics_plus"
  )

  finalDatasources <- allDatabasIds

  if (filterToUsData) {
    finalDatasources <- intersect(
      finalDatasources,
      usDataSources
    )
  }

  if (filterToClaims) {
    finalDatasources <- intersect(
      finalDatasources,
      claimsDataSource
    )
  }

  if (all(
    !is.null(databasIds),
    length(databasIds) > 0
  )) {
    finalDatasources <- intersect(
      finalDatasources,
      databasIds
    )
  }
  return(finalDatasources)
}
