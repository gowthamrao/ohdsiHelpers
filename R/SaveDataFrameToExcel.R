#' Save a DataFrame to an Excel File with Formatting
#'
#' This function saves a given dataframe to an Excel file. It adjusts the width of the columns.
#'
#' @param dataFrame The dataframe to be saved to Excel.
#' @param filePath The file path where the Excel file will be saved.
#' @param sheetName The name of the worksheet in the Excel file (default is "Sheet 1").
#' @export
saveDataFrameToExcel <-
  function(dataFrame, filePath, sheetName = "Sheet 1") {
    # Create a new workbook using openxlsx package
    wb <- openxlsx::createWorkbook()

    # Add a new worksheet to the workbook with the specified sheet name
    openxlsx::addWorksheet(wb, sheetName)

    # Write the dataframe to the specified worksheet
    openxlsx::writeData(wb, sheetName, dataFrame)

    # Determine the number of columns in the dataframe
    numCols <- ncol(dataFrame)

    # Loop through each column to adjust its width to 'auto' for better readability
    # Using setColWidths function from openxlsx package
    openxlsx::setColWidths(wb, sheetName, cols = 1:numCols, widths = "auto")

    # Save the workbook to the specified file path, allowing overwriting if the file already exists
    openxlsx::saveWorkbook(wb, filePath, overwrite = TRUE)
  }
