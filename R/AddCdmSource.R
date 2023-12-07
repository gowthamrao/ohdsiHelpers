#' @export
addCdmSourceBySourceKey <- function(data,
                                    cdmSources) {
  data <- data |>
    dplyr::inner_join(cdmSources |>
                        dplyr::select(sourceKey,
                                      sourceName,
                                      databaseId,
                                      databaseName),
                      by = "source_key")
  
  return(data)
}
