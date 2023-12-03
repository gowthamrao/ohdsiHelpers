#' @export
logarithmicRange <- function(numbers,
                             base = 10) {
  value <- log(
    x = abs(numbers),
    base = 10
  )
  floorValue <- 10^(x <- base::floor(value))
  ceilingValue <- 10^(x <- base::floor(value + 1))


  return(paste0(
    as.character(floorValue),
    " - ",
    as.character(floorValue)
  ))
}
