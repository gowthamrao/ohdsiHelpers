#' @export
readCohortGeneratorOutput <- function(rootFolder,
                                      filters) {
  output <- readOutput(
    rootFolder = rootFolder,
    filters = filters,
    name = "CohortGenerator"
  )

  return(output)
}
