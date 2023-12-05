uploadTempTable <- function(connection,
                            data,
                            tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                            bulkLoad = Sys.getenv("DATABASE_CONNECTOR_BULK_UPLOAD"),
                            camelCaseToSnakeCase = TRUE,
                            dropTableIfExists = TRUE) {
  tempTableName <-
    paste0("#", ConceptSetDiagnostics:::getUniqueString())
  
  invisible(utils::capture.output(
    DatabaseConnector::insertTable(
      connection = connection,
      tableName = tempTableName,
      dropTableIfExists = dropTableIfExists,
      tempTable = TRUE,
      tempEmulationSchema = tempEmulationSchema,
      data = data,
      camelCaseToSnakeCase = camelCaseToSnakeCase,
      bulkLoad = bulkLoad,
      progressBar = TRUE,
      createTable = TRUE
    ),
    file = nullfile()
  ))
  
  return(tempTableName)
}