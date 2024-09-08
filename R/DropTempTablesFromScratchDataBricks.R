#' @export
dropTempTablesFromScratchDataBricks <- function(connectionDetails = NULL,
                                                connection = NULL,
                                                schema = "scratch.scratch_grao9",
                                                string,
                                                exclude = TRUE) {
  disconnectAfter <- FALSE
  if (is.null(connection)) {
    connection <- DatabaseConnector::connect(connectionDetails = connectionDetails)
    disconnectAfter <- TRUE
  }
  # Get the list of tables
  listOfTables <- DatabaseConnector::getTableNames(connection = connection, databaseSchema = schema) |> tolower()
  
  # Filter tables that start with the given string
  filteredTables <- if (exclude) {
    listOfTables[!stringr::str_starts(listOfTables, tolower(string))]
  } else {
    listOfTables[stringr::str_starts(listOfTables, tolower(string))]
  }
  
  # Loop over the filtered list and drop each table
  for (table in filteredTables) {
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = "DROP TABLE IF EXISTS @schema.@table_name;",
      schema = schema,
      table_name = table
    )
  }
  
  if (disconnectAfter) {
    DatabaseConnector::disconnect(connection)
  }
}
