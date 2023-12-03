#' @export
logarithmicRange <- function(numbers,
                             base = 10) {
  value <- log(
    x = abs(numbers),
    base = 10
  )
  floorValue <- 10^(floor(value))
  ceilingValue <- 10^(floor(value + 1))


  return(paste0(
    as.character(floorValue),
    " - ",
    as.character(floorValue)
  ))
}
