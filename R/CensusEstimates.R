#' @export
getUsCensusEstimates <- function() {
  readRDS(file.path(
    "rds",
    "usCensusEstimatesAgeGroupSexCalendarYear.RDS"
  ))
}



updateUsCensusEstimatesUsingApiCall <-
  function(censusApiKey = Sys.getenv("CENSUS_API_KEY")) {
    tidycensus::census_api_key(
      key = censusApiKey,
      overwrite = TRUE,
      install = FALSE
    )

    censusUsEstimate <-
      tidycensus::get_estimates(
        geography = "state",
        product = "characteristics",
        breakdown = c("SEX", "AGEGROUP"),
        breakdown_labels = TRUE,
        time_series = TRUE
      )

    censusUsEstimatePlot <- censusUsEstimate |>
      dplyr::group_by(
        year,
        SEX,
        AGEGROUP
      ) |>
      dplyr::summarise(count = sum(value)) |>
      dplyr::ungroup() |>
      dplyr::rename(
        sex = SEX,
        ageGroup = AGEGROUP
      ) |>
      dplyr::filter(
        sex %in% c("Female", "Male"),
        year == 2020
      ) |>
      dplyr::mutate(
        ageGroup = stringr::str_replace(
          string = ageGroup,
          pattern = "Age|years|and|older",
          replacement = ""
        ) |> stringr::str_squish()
      ) |>
      dplyr::mutate(
        ageGroup = stringr::str_replace(
          string = ageGroup,
          pattern = "years",
          replacement = ""
        ) |> stringr::str_squish()
      )
  }

# useEstimates <- updateUsCensusEstimatesUsingApiCall()
# dir.create(file.path(rstudioapi::getActiveDocumentContext()$path |> dirname() |> dirname(), "inst", "rds"), showWarnings = FALSE, recursive = TRUE)
#
# saveRDS(
#   usEstimates,
#   file = file.path(
#     rstudioapi::getActiveDocumentContext()$path |> dirname() |> dirname(),
#     "inst",
#     "rds",
#     "usCensusEstimatesAgeGroupSexCalendarYear.RDS"
#   )
# )
# censusLabelsOfInterest <- c(paste0("Total!!Female!!",
#                                    c(1:100),
#                                    " years"),
#                             paste0("Total!!Male!!",
#                                    c(1:100),
#                                    " years"))
#
# l2000 <- tidycensus::load_variables(year = 2000, cache = TRUE) |>
#   dplyr::filter(label %in% c(censusLabelsOfInterest)) |>
#   dplyr::filter(concept %in% c("SEX BY AGE [49]", "SEX BY AGE [209]")) |>
#   dplyr::mutate(year = 2000)
#
#
# l2010 <- tidycensus::load_variables(year = 2010, cache = TRUE) |>
#   dplyr::filter(label %in% c(censusLabelsOfInterest)) |>
#   dplyr::filter(concept == "SEX BY AGE") |>
#   dplyr::mutate(year = 2010)
#
#
# l <- dplyr::bind_rows(l2000, l2010) |>
#   dplyr::mutate(sex = dplyr::if_else(
#     condition = stringr::str_detect(string = label,
#                                     pattern = "Female"),
#     false = "Male",
#     true = "Female"
#   )) |>
#   dplyr::mutate(age = stringr::str_extract(label, "\\d+")) |>
#   dplyr::rename(variable = name) |>
#   dplyr::select(year, sex, age, variable) |>
#   dplyr::distinct() |>
#   dplyr::mutate(
#     ageGroup = dplyr::case_when(
#       age >= 0 & age <= 4   ~ "Age 0 to 4 years",
#       age >= 5 & age <= 9   ~ "Age 5 to 9 years",
#       age >= 10 & age <= 14 ~ "Age 10 to 14 years",
#       age >= 15 & age <= 19 ~ "Age 15 to 19 years",
#       age >= 20 & age <= 24 ~ "Age 20 to 24 years",
#       age >= 25 & age <= 29 ~ "Age 25 to 29 years",
#       age >= 30 & age <= 34 ~ "Age 30 to 34 years",
#       age >= 35 & age <= 39 ~ "Age 35 to 39 years",
#       age >= 40 & age <= 44 ~ "Age 40 to 44 years",
#       age >= 45 & age <= 49 ~ "Age 45 to 49 years",
#       age >= 50 & age <= 54 ~ "Age 50 to 54 years",
#       age >= 55 & age <= 59 ~ "Age 55 to 59 years",
#       age >= 60 & age <= 64 ~ "Age 60 to 64 years",
#       age >= 65 & age <= 69 ~ "Age 65 to 69 years",
#       age >= 70 & age <= 74 ~ "Age 70 to 74 years",
#       age >= 75 & age <= 79 ~ "Age 75 to 79 years",
#       age >= 80 & age <= 84 ~ "Age 80 to 84 years",
#       age >= 85             ~ "Age 85 years and older",
#       TRUE                  ~ "Unknown" # Default case
#     )
#   )
#
# censusUs2000 <-
#   tidycensus::get_decennial(geography = "us",
#                             variables = l$variable |> unique(),
#                             year = 2000) |>
#   dplyr::mutate(year = 2000)
# censusUs2010 <-
#   tidycensus::get_decennial(geography = "us",
#                             variables = l$variable |> unique(),
#                             year = 2010) |>
#   dplyr::mutate(year = 2010)
#
# censusOld <- dplyr::bind_rows(censusUs2000,
#                               censusUs2010) |>
#   dplyr::inner_join(l) |>
#   dplyr::select(year,
#                 age,
#                 ageGroup,
#                 sex,
#                 value) |>
#   dplyr::group_by(year,
#                   ageGroup,
#                   sex) |>
#   dplyr::summarise(value = sum(value)) |>
#   dplyr::ungroup() |>
#   dplyr::arrange(year, ageGroup, sex)

# censusUs2020 <- tidycensus::get_decennial(geography = "state", variables = variablesOfInterest, year = 2020)
