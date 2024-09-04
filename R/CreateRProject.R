createRProject <- function(targetFolderPath, projectName) {
  # Check if the target folder exists
  if (!dir.exists(targetFolderPath)) {
    stop("The target folder does not exist.")
  }
  
  # Define the project file path
  rProjFilePath <- file.path(targetFolderPath, paste0(projectName, ".Rproj"))
  
  # Write a minimal .Rproj file
  rProjContent <- c(
    "Version: 1.0",
    "RestoreWorkspace: No",
    "SaveWorkspace: No",
    "AlwaysSaveHistory: No",
    "EnableCodeIndexing: Yes",
    "UseSpacesForTab: Yes",
    "NumSpacesForTab: 2",
    "Encoding: UTF-8"
  )
  
  # Write the .Rproj file to the target folder
  writeLines(rProjContent, rProjFilePath)
  
  message("R project created successfully at: ", rProjFilePath)
}