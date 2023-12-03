#' @export
getCdmSource <- function(cdmSources,
                         database = "optum_extended_dod",
                         sequence = 1) {
  cdmSource <- cdmSources |>
    dplyr::filter(database == !!database) |>
    dplyr::filter(sequence == !!sequence)

  return(cdmSource)
}
