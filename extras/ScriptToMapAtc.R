connectionDetails <- ParallelExecution::createConnectionDetails()
connection <- DatabaseConnector::connect(connectionDetails)

cdmSource <- ParallelExecution::getCdmSource(cdmSources = cdmSources)


atc2ConceptIds <- c(21600960, #ANTITHROMBOTIC AGENTS
               21600381, #ANTIHYPERTENSIVES
               21600712, # DRUGS USED IN DIABETES
               21601853 # LIPID MODIFYING AGENTS
               )


atcConcepts <- DatabaseConnector::renderTranslateQuerySql(
  connection = connection,
  sql = "SELECT concept_id from @vocabulary_database_schema.concept WHERE vocabulary_id = 'ATC';",
  vocabulary_database_schema = cdmSource$cdmDatabaseSchema,
  snakeCaseToCamelCase = TRUE
)

conceptDescendants <- ConceptSetDiagnostics::getConceptDescendant(
  conceptIds = atcConcepts$conceptId,
  connection = connection,
  vocabularyDatabaseSchema = cdmSource$cdmDatabaseSchema
)

mappedSourceConcepts <- ConceptSetDiagnostics::getMappedSourceConcepts(
  conceptIds = c(
    atc2ConceptIds,
    conceptDescendants$ancestorConceptId,
    conceptDescendants$descendantConceptId
  ) |>
    unique(),
  connection = connection,
  vocabularyDatabaseSchema = cdmSource$vocabDatabaseSchema
)

mappedStandardConcepts <- ConceptSetDiagnostics::getMappedSourceConcepts(
  conceptIds = c(
    atc2ConceptIds,
    conceptDescendants$ancestorConceptId,
    conceptDescendants$descendantConceptId,
    mappedSourceConcepts$conceptId
  ) |>
    unique(),
  connection = connection,
  vocabularyDatabaseSchema = cdmSource$vocabDatabaseSchema
)


conceptIdDetails <- ConceptSetDiagnostics::getConceptIdDetails(
  conceptIds = c(
    atc2ConceptIds,
    conceptDescendants$ancestorConceptId,
    conceptDescendants$descendantConceptId,
    mappedStandardConcepts$conceptId
  ) |>
    unique(),
  connection = connection,
  vocabularyDatabaseSchema = cdmSource$vocabDatabaseSchema
)

atcConceptsLevel2 <- conceptIdDetails |>
  dplyr::filter(conceptId %in% c(atc2ConceptIds)) |>
  dplyr::filter(vocabularyId == "ATC") |>
  dplyr::filter(conceptClassId == "ATC 2nd")

atcConceptsLevel3 <- conceptIdDetails |>
  dplyr::filter(vocabularyId == "ATC") |>
  dplyr::filter(conceptClassId == "ATC 3rd")

atcConceptsLevel4 <- conceptIdDetails |>
  dplyr::filter(vocabularyId == "ATC") |>
  dplyr::filter(conceptClassId == "ATC 4th")

atcConceptsLevel5 <- conceptIdDetails |>
  dplyr::filter(vocabularyId == "ATC") |>
  dplyr::filter(conceptClassId == "ATC 5th")




atcLevels <- atcConceptsLevel2 |>
  dplyr::select(conceptId, conceptName) |>
  dplyr::rename(ancestorConceptId = conceptId, atc2 = conceptName) |>
  dplyr::left_join(
    conceptDescendants |>
      dplyr::filter(descendantConceptId %in% c(atcConceptsLevel3$conceptId)) |>
      dplyr::select(ancestorConceptId, descendantConceptId) |>
      dplyr::rename(conceptId = descendantConceptId) |>
      dplyr::inner_join(conceptIdDetails |>
                          dplyr::select(conceptId, conceptName))
  ) |>
  dplyr::rename(
    atc2ConceptId = ancestorConceptId,
    ancestorConceptId = conceptId,
    atc3 = conceptName
  ) |>
  dplyr::left_join(
    conceptDescendants |>
      dplyr::filter(descendantConceptId %in% c(atcConceptsLevel4$conceptId)) |>
      dplyr::select(ancestorConceptId, descendantConceptId) |>
      dplyr::rename(conceptId = descendantConceptId) |>
      dplyr::inner_join(conceptIdDetails |>
                          dplyr::select(conceptId, conceptName))
  ) |>
  dplyr::rename(
    atc3ConceptId = ancestorConceptId,
    ancestorConceptId = conceptId,
    atc4 = conceptName
  ) |>
  dplyr::left_join(
    conceptDescendants |>
      dplyr::filter(descendantConceptId %in% c(atcConceptsLevel5$conceptId)) |>
      dplyr::select(ancestorConceptId, descendantConceptId) |>
      dplyr::rename(conceptId = descendantConceptId) |>
      dplyr::inner_join(conceptIdDetails |>
                          dplyr::select(conceptId, conceptName))
  ) |>
  dplyr::rename(atc4ConceptId = ancestorConceptId,
                atc5ConceptId = conceptId,
                atc5 = conceptName)


mappedIngredients <- ConceptSetDiagnostics::getMappedStandardConcepts(
  conceptIds = c(atcLevels$atc5ConceptId) |>
    unique(),
  connection = connection,
  vocabularyDatabaseSchema = cdmSource$vocabDatabaseSchema
)


atcLevels <- atcLevels |>
  dplyr::left_join(
    mappedIngredients |>
      dplyr::select(givenConceptId, conceptId, conceptName) |>
      dplyr::rename(
        rxNormConceptId = conceptId,
        rxNormConceptName = conceptName,
        atc5ConceptId = givenConceptId
      )
  )


OhdsiHelpers::saveDataFrameToExcel(dataFrame = atcLevels, filePath = "atc2ToRxNorm.xlsx")
