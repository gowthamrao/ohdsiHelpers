getConsortCohortParent <- function(cohortDefinitionSet) {
  relationships <-
    cohortDefinitionSet |>
    dplyr::select(cohortId,
                  cohortName) |>
    tidyr::crossing(
      cohortDefinitionSet |>
        dplyr::select(cohortName) |>
        dplyr::rename(subString = cohortName)
    ) |>
    dplyr::filter(cohortName != subString) |>
    dplyr::filter(stringr::str_starts(string = cohortName,
                                      pattern = stringr::fixed(subString))) |>
    dplyr::mutate(proportion = nchar(subString) / nchar(cohortName)) |>
    dplyr::group_by(cohortId,
                    cohortName) |>
    dplyr::arrange(dplyr::desc(proportion), .by_group = TRUE) |>
    dplyr::mutate(rn = dplyr::row_number()) |>
    dplyr::filter(rn == 1) |>
    dplyr::ungroup() |>
    dplyr::select(-rn,-proportion, -cohortName) |>
    dplyr::inner_join(
      cohortDefinitionSet |>
        dplyr::select(cohortId,
                      cohortName) |>
        dplyr::rename(subString = cohortName,
                      consortParent = cohortId),
      by = "subString"
    ) |>
    dplyr::rename(consortParentCohortName = subString)
  
  cohortDefinitionSet <- cohortDefinitionSet |>
    dplyr::left_join(relationships,
                     by = "cohortId") |>
    dplyr::mutate(
      newName = stringr::str_replace(
        string = cohortName,
        pattern = stringr::fixed(consortParentCohortName),
        replacement = "     "
      )
    ) |>
    dplyr::mutate(newName = removeLeadingCharacters(newName, " , ")) |>
    dplyr::mutate(newName = removeLeadingCharacters(newName, "-  |"))
  
  return(cohortDefinitionSet)
  
}