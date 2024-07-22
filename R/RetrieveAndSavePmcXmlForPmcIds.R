#' Retrieve and Save PMC XML Using Multiple Methods
#'
#' This function attempts to retrieve XML data for a list of PMC IDs using the tidy PMC method first.
#' If the tidy PMC method fails, it then tries using the Rentrez method. It saves the retrieved XML
#' to files in directories corresponding to the retrieval method used within a specified output folder.
#'
#' @param pmcIds A vector of PMC IDs for which to retrieve XML data.
#' @param outputFolder A character string specifying the output folder location. The default is
#' the current working directory.
#' @param incremental A logical value indicating whether to skip retrieval if the output file already
#' exists. Default is \code{TRUE}.
#'
#' @return A list with two elements:
#' \itemize{
#'   \item{success}{A list of successfully retrieved and saved XML contents, indexed by PMC ID.}
#'   \item{failed}{A list of PMC IDs for which XML retrieval failed using both methods, set to \code{NA}.}
#' }
#'
#' @export
retrieveAndSavePmcXmlForPmcIds <-
  function(pmcIds,
           outputFolder = ".",
           incremental = TRUE) {
    xmlList <- list()
    failures <- list()
    
    for (pmcId in pmcIds) {
      methodUsed <- "tidyPmc"
      outputDir <- file.path(outputFolder, methodUsed)
      outputFile <- file.path(outputDir, paste0(pmcId, ".xml"))
      
      if (incremental && file.exists(outputFile)) {
        writeLines(paste0("File already exists for ", pmcId, ". Skipping."))
        next
      }
      
      xmlContent <-
        OhdsiHelpers::retrievePMCXMLUsingTidyPmc(pmcId = pmcId)
      
      if (is.null(xmlContent)) {
        writeLines(paste0(
          "Failed to retrieve XML for ",
          pmcId,
          " using tidy PMC, trying Rentrez."
        ))
        xmlContent <- retrievePMCXMLUsingRentrez(pmcId = pmcId)
        methodUsed <- "rentrez"
        outputDir <- file.path(outputFolder, methodUsed)
        outputFile <- file.path(outputDir, paste0(pmcId, ".xml"))
      }
      
      dir.create(outputDir, recursive = TRUE, showWarnings = FALSE)
      
      if (!is.null(xmlContent)) {
        if (methodUsed == "tidyPmc") {
          xml2::write_xml(x = xmlContent, file = outputFile)
        } else {
          XML::saveXML(doc = xmlContent, file = outputFile)
        }
        xmlList[[pmcId]] <- xmlContent
        writeLines(paste0("Saved XML for ", pmcId, " to ", outputFile))
      } else {
        writeLines(paste0("Failed to retrieve XML for ", pmcId, " using both methods."))
        failures[[pmcId]] <- NA
      }
    }
    
    list(success = xmlList, failed = failures)
  }