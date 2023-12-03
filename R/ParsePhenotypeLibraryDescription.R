#' @export
parsePhenotypeLibraryDescription <- function(cohortDefinition) {
  librarian <- stringr::str_replace(
    string = cohortDefinition$createdBy$name,
    pattern = "na\\\\",
    replacement = ""
  )
  cohortName <- cohortDefinition$name
  cohortNameFormatted <- gsub(
    pattern = "_",
    replacement = " ",
    x = gsub("\\[(.*?)\\]_", "", gsub(" ", "_", cohortName))
  ) |>
    stringr::str_squish() |>
    stringr::str_trim()

  if (length(cohortDefinition$modifiedBy) > 1) {
    lastModifiedBy <-
      cohortDefinition$modifiedBy$name
  }

  output <- dplyr::tibble(
    librarian = librarian,
    cohortName = cohortName,
    cohortNameFormatted = cohortNameFormatted,
    lastModifiedBy = lastModifiedBy
  )

  if (all(
    !is.na(cohortDefinition$description),
    nchar(cohortDefinition$description) > 5
  )) {
    textInDescription <-
      cohortDefinition$description |>
      stringr::str_replace_all(pattern = ";", replacement = "") |>
      stringr::str_split(pattern = "\n")
    strings <- textInDescription[[1]]
    textInDescription <- NULL

    strings <-
      stringr::str_split(string = strings, pattern = stringr::fixed(":"))

    if (all(
      !is.na(cohortDefinition$description[[1]]),
      stringr::str_detect(
        string = cohortDefinition$description,
        pattern = stringr::fixed(":")
      )
    )) {
      stringValues <- c()
      for (j in (1:length(strings))) {
        stringValues[[j]] <- dplyr::tibble()
        if (length(strings[[j]]) == 2) {
          stringValues[[j]] <- dplyr::tibble(
            name = strings[[j]][[1]] |> stringr::str_squish() |> stringr::str_trim(),
            value = strings[[j]][[2]] |>
              stringr::str_squish() |>
              stringr::str_trim()
          )
        }
      }

      stringValues <- dplyr::bind_rows(stringValues)

      if (nrow(stringValues) > 0) {
        data <- stringValues |>
          tidyr::pivot_wider()
        stringValues <- NULL

        output <- output |>
          dplyr::select(setdiff(
            x = colnames(output),
            y = colnames(data)
          )) |>
          tidyr::crossing(data)
      }
    }
  }
  return(output)
}
