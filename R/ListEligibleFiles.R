#' @export
listEligibleFiles <- function(path,
                              name) {
  listOfFiles <- dplyr::tibble(
    fullName = list.files(
      path = path,
      pattern = paste0(name, ".RDS"),
      full.names = TRUE,
      include.dirs = TRUE,
      recursive = TRUE
    )
  ) |>
    dplyr::mutate(
      searchName = name,
      folderName = basename(gsub(
        pattern = paste0("/", name, ".RDS"),
        x = .data$fullName,
        replacement = ""
      ))
    )

  listOfFiles <- listOfFiles |>
    dplyr::filter(stringr::str_detect(
      string = tolower(.data$fullName),
      pattern = "combined",
      negate = TRUE
    ))
  return(listOfFiles)
}
