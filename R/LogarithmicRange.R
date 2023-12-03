#' @export
logarithmicRange <- function(numbers,
                             base = 10) {
  value <- log(
    x = abs(numbers),
    base = 10
  )
  floor <- 10^(x <- floor(value))
  ceiling <- 10^(x <- floor(value + 1))


  return(paste0(
    as.character(floor),
    " - ",
    as.character(ceiling)
  ))
}
