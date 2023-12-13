#' @export
uploadToPostgres <- function(pathWithZipFiles,
                             connectionDetails,
                             outputModel = "CohortDiagnostics",
                             tablePrefix = "cd_",
                             createSchemaIfNotExists = TRUE,
                             resultsDatabaseSchema,
                             dropOldTables = FALSE,
                             createTables = TRUE,
                             ignoreCombined = TRUE,
                             readOnlyAccountToGrant = Sys.getenv("ohdaResultsReadOnlyUser")) {
  if (outputModel == "CohortDiagnostics") {
    resultsModelSpecifications = CohortDiagnostics::getResultsDataModelSpecifications()
    additionalTables = c(
      'annotation',
      'annotation_attributes',
      'annotation_link',
      'migration',
      'package_version'
    )
    # reading the tables in cohort diagnostics results data model
    tablesInResultsDataModel <-
      resultsModelSpecifications |>
      dplyr::select(tableName) |>
      dplyr::distinct() |>
      dplyr::arrange() |>
      dplyr::pull()
    tablesInResultsDataModel <- c(tablesInResultsDataModel,
                                  additionalTables)
  } else {
    stop("Only Cohort Diagnostics supported")
  }
  
  connection <-
    DatabaseConnector::connect(connectionDetails = connectionDetails)
  
  if (createSchemaIfNotExists) {
    DatabaseConnector::renderTranslateExecuteSql(
      connection = connection,
      sql = "CREATE SCHEMA IF NOT EXISTS @results_database_schema;",
      results_database_schema = resultsDatabaseSchema
    )
  }
  
  if (dropOldTables) {
    for (i in (1:length(tablesInResultsDataModel))) {
      writeLines(paste0("Dropping table ", tablesInResultsDataModel[[i]]))
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = "DROP TABLE IF EXISTS @database_schema.@prefix@table_name CASCADE;",
        database_schema = resultsDatabaseSchema,
        table_name = tablesInResultsDataModel[[i]],
        prefix = tablePrefix
      )
    }
  }
  
  if (createTables) {
    CohortDiagnostics::createResultsDataModel(
      connectionDetails = connectionDetails,
      databaseSchema = resultsDatabaseSchema,
      tablePrefix = tablePrefix
    )
  }
  
  listOfZipFilesToUpload <-
    list.files(
      path = pathWithZipFiles,
      pattern = ".zip",
      full.names = TRUE,
      recursive = TRUE
    )
  
  if (ignoreCombined) {
    listOfZipFilesToUpload <-
      listOfZipFilesToUpload[stringr::str_detect(string = listOfZipFilesToUpload,
                                                 pattern = "Combined",
                                                 negate = TRUE)]
  }
  
  for (i in (1:length(listOfZipFilesToUpload))) {
    tempLocation <- file.path(tempdir(), basename(tempfile()))
    unlink(x = tempLocation,
           recursive = TRUE,
           force = TRUE)
    dir.create(tempLocation)
    if (outputModel == "CohortDiagnostics") {
      CohortDiagnostics::uploadResults(
        connectionDetails = connectionDetails,
        schema = resultsDatabaseSchema,
        zipFileName = listOfZipFilesToUpload[[i]],
        purgeSiteDataBeforeUploading = TRUE,
        tempFolder = tempLocation,
        tablePrefix = tablePrefix
      )
    }
    unlink(x = tempLocation,
           recursive = TRUE,
           force = TRUE)
  }
  
  if (!is.null(readOnlyAccountToGrant)) {
    sqlGrant <-
      " GRANT SELECT ON ALL TABLES IN SCHEMA @results_database_schema to @read_only_account;
        GRANT USAGE ON SCHEMA @results_database_schema TO @read_only_account;
        GRANT SELECT ON ALL TABLES IN SCHEMA @results_database_schema TO @read_only_account;
        GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA @results_database_schema TO @read_only_account;"
    sqlGrant <-
      SqlRender::render(
        sql = sqlGrant,
        results_database_schema = resultsDatabaseSchema,
        read_only_account = readOnlyAccountToGrant
      )
    DatabaseConnector::executeSql(connection = connection,
                                  sql = sqlGrant)
  }
  
  # Maintenance
  for (i in (1:length(tablesInResultsDataModel))) {
    # vacuum
    try(if (DatabaseConnector::existsTable(
      connection = connection,
      databaseSchema = resultsDatabaseSchema,
      tableName = tablesInResultsDataModel[[i]]
    )) {
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        sql = "VACUUM VERBOSE ANALYZE @database_schema.@table_name;",
        database_schema = resultsDatabaseSchema,
        table_name = tablesInResultsDataModel[[i]]
      )
    })
  }
  
  DatabaseConnector::disconnect(connection = connection)
}
