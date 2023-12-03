# Format and check code ---------------------------------------------------
styler::style_pkg()
OhdsiRTools::checkUsagePackage("OhdsiHelpers")


devtools::check()


# Release package to CRAN ------------------------------------------------------
devtools::check_win_devel()
devtools::check_rhub()