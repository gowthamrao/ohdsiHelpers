# Format and check code ---------------------------------------------------
styler::style_pkg()
OhdsiRTools::checkUsagePackage("OhdsiHelpers")

# Devtools check -----------------------------------------------------------
devtools::spell_check()
devtools::check()

# Create manual -----------------------------------------------------------
unlink("extras/CohortExplorer.pdf")
shell("R CMD Rd2pdf ./ --output=extras/CohortExplorer.pdf")
dir.create(path = file.path("inst", "doc"), showWarnings = FALSE, recursive = TRUE)

# Create Vignettes---------------------------------------------------------
rmarkdown::render("vignettes/HowToUseCohortExplorer.Rmd",
                  output_file = "../inst/doc/HowToUseCohortExplorer.pdf",
                  rmarkdown::pdf_document(latex_engine = "pdflatex",
                                          toc = TRUE,
                                          number_sections = TRUE))


# Build site---------------------------------------------------------
devtools::document()
pkgdown::build_site()
OhdsiRTools::fixHadesLogo()



# Release package to CRAN ------------------------------------------------------
devtools::check_win_devel()
devtools::check_rhub()
devtools::release()
devtools::check(cran=TRUE)