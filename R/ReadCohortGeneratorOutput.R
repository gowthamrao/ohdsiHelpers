#' @export
readCohortGeneratorOutput <- function(rootFolder,
                                      filters) {
  output <- readOutput(
    rootFolder = rootFolder,
    filter = filters,
    name = "CohortGenerator"
  )

  return(output)
}
