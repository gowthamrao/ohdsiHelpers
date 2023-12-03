#' @export
prettyTable <- function(dataFrame,
                        caption = "Table",
                        align = "l") {
  knitr::kable(dataFrame,
               caption = caption,
               format = "html",
               align = align) |>
    kableExtra::kable_styling(bootstrap_options = c("striped", "hover"))
}