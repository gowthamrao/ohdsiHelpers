#' @export
# Function to dynamically split a string column and rename the resulting columns
splitStringAndRenameColumns <-
  function(dataFrame,
           targetColumnName,
           enableConsecutiveMerge = TRUE,
           splitBy = " - ",
           delimiterForMerge = "     ") {
    # Split the target column in the data frame by value of splitBy and convert the result into a list
    splitComponentsList <-
      strsplit(dataFrame[[targetColumnName]], split = splitBy)
    
    # Determine the maximum number of split components across all rows
    maxSplitCount <- max(sapply(splitComponentsList, length))
    
    # Generate dynamic column names based on the maximum number of splits
    dynamicColumnNames <-
      paste0("component", seq_len(maxSplitCount))
    
    # Transform the list of split components into a data frame, ensuring uniform column lengths
    splitComponentsDataFrame <-
      do.call(rbind,
              lapply(splitComponentsList, `length<-`, maxSplitCount))
    splitComponentsDataFrame <-
      as.data.frame(splitComponentsDataFrame, stringsAsFactors = FALSE)
    names(splitComponentsDataFrame) <- dynamicColumnNames
    
    # Combine the newly split columns with the original data frame, minus the split column
    combinedDataFrame <- dataFrame |>
      dplyr::select(-dplyr::all_of(targetColumnName)) |>
      dplyr::bind_cols(splitComponentsDataFrame) |>
      dplyr::mutate(dplyr::across(dplyr::all_of(dynamicColumnNames), tidyr::replace_na, "")) |>
      dplyr::arrange(dplyr::across(dplyr::all_of(dynamicColumnNames))) |>
      dplyr::mutate(id = dplyr::row_number()) |>
      dplyr::relocate(id)
    
    # Function to find the root column name based on a specific pattern
    findRootColumnName <- function(dataFrame) {
      columnNamesMatchingPattern <-
        colnames(dataFrame)[stringr::str_detect(string = colnames(dataFrame), pattern = "component")] |> unique() |> sort()
      for (columnName in columnNamesMatchingPattern) {
        if (length(dataFrame[[columnName]] |> unique()) == 1) {
          rootName <- columnName
        }
      }
      return(rootName)
    }
    
    # Function to ensure the presence of a root name within the data frame
    integrateRootName <- function(dataFrame, rootColumnName) {
      columnNamesMatchingPattern <-
        colnames(dataFrame)[stringr::str_detect(string = colnames(dataFrame), pattern = "component")] |> unique() |> sort()
      columnsBeforeRoot <-
        columnNamesMatchingPattern[columnNamesMatchingPattern <= rootColumnName]
      columnsAfterRoot <-
        setdiff(columnNamesMatchingPattern, columnsBeforeRoot)
      columnsAfterRootFirst <-
        if (length(columnsAfterRoot) >= 1)
          columnsAfterRoot[[1]]
      else
        NULL
      
      updatedDataFrame <- dataFrame |>
        dplyr::rowwise() |>
        dplyr::mutate(rootName = paste0(dplyr::across(dplyr::all_of(
          c(columnsBeforeRoot, columnsAfterRootFirst)
        )), collapse = splitBy)) |>
        dplyr::ungroup() |>
        dplyr::select(-dplyr::all_of(c(
          columnsBeforeRoot, columnsAfterRootFirst
        ))) |>
        dplyr::relocate(id)
      return(updatedDataFrame)
    }
    
    rootColumnName <-
      findRootColumnName(dataFrame = combinedDataFrame)
    combinedDataFrame <-
      integrateRootName(dataFrame = combinedDataFrame, rootColumnName = rootColumnName)
    
    relevantColumnNames <-
      intersect(dynamicColumnNames, colnames(combinedDataFrame))
    
    # Function to remove consecutive duplicate values in specified columns
    removeConsecutiveDuplicates <-
      function(dataFrame, columnNames) {
        for (columnName in columnNames) {
          if (columnName %in% names(dataFrame)) {
            previousValue <- dataFrame[[1, columnName]]
            for (rowIndex in 2:nrow(dataFrame)) {
              if (dataFrame[[rowIndex, columnName]] == previousValue) {
                dataFrame[[rowIndex, columnName]] <- ""
              } else {
                previousValue <- dataFrame[[rowIndex, columnName]]
              }
            }
          } else {
            warning(paste("Column", columnName, "not found in the data frame."))
          }
        }
        return(dataFrame)
      }
    
    # Function to concatenate specified columns with a given delimiter
    mergeColumnsWithDelimiter <-
      function(dataFrame, columnNames, delimiter) {
        # Ensure all specified columns exist in the data frame
        if (!all(columnNames %in% colnames(dataFrame))) {
          stop("Not all specified columns exist in the data frame.")
        }
        
        # Concatenate columns with the specified delimiter
        mergedColumn <-
          Reduce(function(x, y) {
            paste0(x, delimiter, y)
          }, dataFrame[columnNames])
        
        # Add the merged column to the data frame and adjust its placement
        dataFrame$mergedColumn <- mergedColumn
        dataFrame <- dataFrame |>
          dplyr::select(-columnNames) |>
          dplyr::mutate(mergedColumn = stringr::str_trim(string = .data$mergedColumn, side = "right"))
        
        return(dataFrame)
      }
    
    # If enabled, process data frame to merge consecutive duplicates and concatenate columns
    if (enableConsecutiveMerge) {
      combinedDataFrame <-
        removeConsecutiveDuplicates(
          dataFrame = combinedDataFrame,
          columnNames = c("rootName", relevantColumnNames)
        )
      combinedDataFrame <-
        mergeColumnsWithDelimiter(
          dataFrame = combinedDataFrame,
          columnNames = c("rootName", relevantColumnNames),
          delimiter = delimiterForMerge
        )
    }
    
    # Return the processed data frame
    return(combinedDataFrame)
  }
