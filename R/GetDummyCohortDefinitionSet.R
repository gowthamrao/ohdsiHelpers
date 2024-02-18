#' @export
getDummyCohortDefinitionSet <- function() {
  # OhdsiHelpers::authorizeToJnjAtlas()
  # cohortDefinitionSet <- ROhdsiWebApi::exportCohortDefinitionSet(
  #   baseUrl = Sys.getenv("BaseUrl"),
  #   cohortIds = 15726,
  #   generateStats = FALSE
  # ) |> 
  #   dplyr::select(sql,
  #                 json) |> 
  #   dplyr::mutate(cohortId = -999,
  #                 cohortName = "dummary cohort") |> 
  #   dplyr::relocate(cohortId,
  #                   cohortName)
  # 
  # saveRDS(object = cohortDefinitionSet, file = "inst/DummyCohortDefinitionSet.RDS")
  
  readRDS(file = system.file("DummyCohortDefinitionSet.RDS",
                             package = utils::packageName()))
}