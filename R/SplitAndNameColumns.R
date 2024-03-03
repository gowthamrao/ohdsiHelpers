#' @export
# Function to split the string and name columns dynamically
splitAndNameColumns <-
  function(dataFrame, columnName, makeConsort = TRUE, seperateSplitsBy = "    ") {
    # Split the specified column in the data frame by ' - ' and convert the result into a list.
    # Each element of the list corresponds to a row in the data frame, and contains
    # the split parts of the string in that row.
    splitList <- strsplit(dataFrame[[columnName]], split = " - ")

    # Determine the maximum number of parts obtained by splitting across all rows.
    # This is used to ensure all rows have the same number of columns after splitting.
    maxSplits <- max(sapply(splitList, length))

    # Create dynamic column names for the new columns. These are named 'name1', 'name2', etc.,
    # up to the number of maximum splits.
    dfColumnNames <- paste0("name", seq_len(maxSplits))

    # Convert the list of split parts into a data frame. Missing values are filled with NA,
    # and then the NAs are standardized to ensure all rows have the same length.
    # Finally, set the names of the new columns as defined above.
    splitDataFrame <-
      do.call(rbind, lapply(splitList, `length<-`, maxSplits))
    splitDataFrame <-
      as.data.frame(splitDataFrame, stringsAsFactors = FALSE)
    names(splitDataFrame) <- dfColumnNames

    # Bind the new split columns to the original data frame after removing the column that was split.
    # Replace NA values in the new columns with empty strings.
    # Sort the data frame based on the values in the new columns.
    # Add a new column 'id' that contains row numbers.
    # Relocate the 'id' column to the first position.
    result <- dataFrame |>
      dplyr::select(-dplyr::all_of(columnName)) |>
      dplyr::bind_cols(splitDataFrame) |>
      dplyr::mutate(dplyr::across(dplyr::all_of(dfColumnNames), tidyr::replace_na, "")) |>
      dplyr::arrange(dplyr::across(dplyr::all_of(dfColumnNames))) |>
      dplyr::mutate(id = dplyr::row_number()) |>
      dplyr::relocate(id)
    
    rootColumnName <- function(df) {
      colNamesInDf <-
        colnames(df)[stringr::str_detect(string = colnames(df), pattern = "name")] |> unique() |> sort()
      for (i in (1:length(colNamesInDf))) {
        colNameInDf <- colNamesInDf[[i]]
        
        if (length(df[[colNameInDf]] |> unique()) == 1) {
          rootName <- colNamesInDf[[i]]
        }
      }
      return(rootName)
    }
    
    ensureRootName <- function(df, rootName) {
      colNamesInDf <-
        colnames(df)[stringr::str_detect(string = colnames(df), pattern = "name")] |> unique() |> sort()
      colNamesInDfToCollapse <-
        colNamesInDf[colNamesInDf <= rootName]
      colNamesInDfAfter <- setdiff(colNamesInDf, colNamesInDfToCollapse)
      if (length(colNamesInDfAfter) >= 1) {
        colNamesInDfAfter <- colNamesInDfAfter[[1]]
      } else {
        colNamesInDfAfter <- NULL
      }
      
      result <- result |>
        dplyr::rowwise() |>
        dplyr::mutate(rootName = paste0(dplyr::across(dplyr::all_of(
          c(colNamesInDfToCollapse,
            colNamesInDfAfter)
        )), collapse = " - ")) |>
        dplyr::ungroup() |>
        dplyr::select(-dplyr::all_of(c(
          colNamesInDfToCollapse,
          colNamesInDfAfter
        ))) |>
        dplyr::relocate(id, cohortId,
                        rootName)
      return(result)
    }
    
    rootName <- rootColumnName(df = result)
    result <- ensureRootName(df = result, rootName = rootName)
    
    dfColumnNames <- intersect(dfColumnNames, colnames(result))
    
    
    clearConsecutiveValues <- function(dataFrame, columnNames) {
      for (columnName in columnNames) {
        if (columnName %in% names(dataFrame)) {
          currentValue <- dataFrame[[1, columnName]]
          for (i in 2:nrow(dataFrame)) {
            if (dataFrame[[i, columnName]] == currentValue) {
              dataFrame[[i, columnName]] <- ""
            } else {
              currentValue <- dataFrame[[i, columnName]]
            }
          }
        } else {
          warning(paste("Column", columnName, "not found in the data frame."))
        }
      }
      return(dataFrame)
    }

    concatenateColumnsWithSpace <-
      function(dataFrame, columnNames, seperateSplitsBy) {
        # Check if all column names exist in the data frame
        if (!all(columnNames %in% colnames(dataFrame))) {
          stop("Not all specified columns exist in the data frame")
        }

        # Use Reduce function to iteratively concatenate columns with four spaces in between
        concatenatedColumn <-
          Reduce(function(x, y) {
            paste0(x, seperateSplitsBy, y)
          }, dataFrame[columnNames])

        # Add the concatenated column to the data frame
        dataFrame$newColumn <- concatenatedColumn

        dataFrame <- dataFrame |>
          dplyr::select(-columnNames) |>
          dplyr::mutate(newColumn = stringr::str_trim(string = .data$newColumn, side = "right"))

        return(dataFrame)
      }

    # If makeConsort is TRUE, remove duplicate adjacent values in new columns.
    if (makeConsort) {
      result <-
        clearConsecutiveValues(dataFrame = result, columnName = dfColumnNames)

      result <-
        concatenateColumnsWithSpace(dataFrame = result, columnName = dfColumnNames, seperateSplitsBy = seperateSplitsBy)
    }
    # Return the modified data frame.
    return(result)
  }
