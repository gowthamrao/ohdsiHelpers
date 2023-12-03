#' @export
readOutput <- function(rootFolder,
                       name = "",
                       filters = NULL) {
  listOfFiles <- dplyr::tibble(
    fullName = list.files(
      path = rootFolder,
      pattern = paste0(name, ".RDS"),
      full.names = TRUE,
      include.dirs = FALSE,
      recursive = FALSE
    )
  ) |>
    dplyr::filter(stringr::str_detect(
      string = fullName,
      pattern = paste0(name, ".RDS")
    ))

  if (nrow(listOfFiles) > 1) {
    stop(paste0("more than one ", name, "output found"))
  }
  if (nrow(listOfFiles) == 0) {
    stop(paste0("no ", name, "output found"))
  }
  output <- readRDS(listOfFiles[1, ]$fullName)


  if (!is.null(filters)) {
    outputNames <- names(output)
    for (i in (1:length(outputNames))) {
      if (intersect(
        colnames(filters),
        colnames(output[[outputNames[[i]]]])
      ) == colnames(filters)) {
        output[[outputNames[[i]]]] <- output[[outputNames[[i]]]] |>
          dplyr::inner_join(filters |>
            dplyr::distinct())
      }
    }
  }

  return(output)
}
