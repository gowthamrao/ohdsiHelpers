#' @export
getCdmSource <- function(cdmSources,
                         database = "optum_extended_dod",
                         sequence = 1) {
  
  if (!is.null(database)) {
    cdmSources <- cdmSources |>
      dplyr::filter(database %in% c(!!database))
  }
  if (!is.null(sequence)) {
    cdmSources <- cdmSources |>
      dplyr::filter(sequence %in% c(!!sequence))
  }

  return(cdmSources)
}
