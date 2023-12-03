#' @export
pheValuatorRenameEvaluationObjects <- function(outputFolder) {
  time <- ceiling((Sys.time() |> as.double()) * 10000)
  
  findAndRename <- function(search) {
    fileName <- strsplit(search, "\\.")[[1]][[1]]
    extension <- strsplit(search, "\\.")[[1]][[2]]
    
    evaluationCohortMailFiles <-
      list.files(
        path = outputFolder,
        pattern = search,
        full.names = TRUE,
        recursive = TRUE
      )
    
    if (length(evaluationCohortMailFiles) > 0) {
      for (i in (1:length(evaluationCohortMailFiles))) {
        file.rename(from = evaluationCohortMailFiles[[i]],
                    to = file.path(
                      dirname(evaluationCohortMailFiles[[i]]),
                      paste0(fileName,
                             "_",
                             time,
                             ".",
                             extension)
                    ))
      }
    }
  }
  
  findAndRename(search = "evaluationCohort_main.rds")
  findAndRename(search = "pv_algorithm_performance_results.csv")
  findAndRename(search = "pv_diagnostics.csv")
  findAndRename(search = "pv_evaluation_input_parameters.csv")
  findAndRename(search = "pv_test_subjects.csv")
  findAndRename(search = "pv_test_subjects_covariates.csv")
  findAndRename(search = "pv_algorithm_performance_results_prevalence.csv")
  
}