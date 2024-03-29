#' #' @export
#' CohortDaysDistributionCummulativeDays <-
#'   function(connection = NULL) {
#'     sql <- "
#'   WITH DailyCounts AS (
#'   SELECT
#'     DATEDIFF(day, MIN(cohort_start_date), MAX(cohort_start_date)) AS days,
#'     COUNT(DISTINCT subject_id) as persons
#'   FROM
#'     scratch_r_optum_extended_dod.intoCohorts
#'   WHERE
#'     cohort_definition_id = 16128
#'   GROUP BY
#'     subject_id
#' ),
#' CumulativeSums AS (
#'   SELECT
#'     days,
#'     persons,
#'     SUM(persons) OVER (ORDER BY days ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_persons
#'   FROM
#'     DailyCounts
#' ),
#' TotalPersons AS (
#'   SELECT
#'     SUM(persons) as total_persons
#'   FROM
#'     DailyCounts
#' ),
#' CumulativePercent AS (
#'   SELECT
#'     days,
#'     persons,
#'     cumulative_persons,
#'     (CAST(cumulative_persons AS FLOAT) / (SELECT total_persons FROM TotalPersons)) * 100 AS cumulative_percent
#'   FROM
#'     CumulativeSums
#' )
#' SELECT
#'   *
#' FROM
#'   CumulativePercent
#' ORDER BY
#'   days;
#' "
#'   }
