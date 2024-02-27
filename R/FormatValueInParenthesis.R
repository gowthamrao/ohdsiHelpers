#' @export
formatMeanSd <- function(mean, standardDeviation, digits = 2) {
  result <-
    paste(
      mean,
      "(SD =",
      formatDecimalWithComma(number = standardDeviation, decimalPlaces = digits),
      ")"
    )
  return(result)
}


#' @export
formatCountPercent <- function(count, percent, percentDigits = 1) {
  return(paste0(
    formatIntegerWithComma(count),
    " (",
    formatPercent(percent, digits = percentDigits),
    ")"
  ))
}

#' @export
formatIntegerWithComma <- function(number) {
  return(formatC(number, format = "d", big.mark = ","))
}

#' @export
formatPercent <- function(x,
                          digits = 2,
                          format = "f",
                          ...) {
  paste0(formatC(100 * x, format = format, digits = digits, ...), "%")
}

#' @export
formatDecimalWithComma <-
  function(number,
           decimalPlaces = 1,
           round = TRUE) {
    integerPart <- floor(number)
    decimalPart <- number - integerPart
    
    if (round) {
      decimalPart <- round(decimalPart, decimalPlaces)
    } else {
      decimalPart <-
        trunc(decimalPart * 10 ^ decimalPlaces) / 10 ^ decimalPlaces
    }
    
    formattedIntegerPart <-
      formatC(integerPart, format = "d", big.mark = ",")
    decimalPartAsString <-
      formatC(decimalPart, format = "f", digits = decimalPlaces)
    formattedDecimalPart <-
      substr(decimalPartAsString, 3, nchar(decimalPartAsString))
    
    return(paste(formattedIntegerPart, formattedDecimalPart, sep = "."))
  }
