# Cohort Subset time windows
cohortSubsetWindowStartAnytimeBeforeToAnytimeAfter <-
  CohortGenerator::createSubsetCohortWindow(startDay = -9999,
                                            endDay = 9999,
                                            targetAnchor = "cohortStart")
# on or before-----
cohortSubsetWindowStartAnytimeBeforeToOnDay <-
  CohortGenerator::createSubsetCohortWindow(startDay = -9999,
                                            endDay = 0,
                                            targetAnchor = "cohortStart")
# on or before 365-----
cohortSubsetWindowStart365BeforeToOnDay <-
  CohortGenerator::createSubsetCohortWindow(startDay = -365,
                                            endDay = 0,
                                            targetAnchor = "cohortStart")
# before-----
cohortSubsetWindowStartAnyTimeBeforeTo1DayBefore <-
  CohortGenerator::createSubsetCohortWindow(startDay = -9999,
                                            endDay = -1,
                                            targetAnchor = "cohortStart")
# before 365-----
cohortSubsetWindowStart365DayBeforeTo1DayBefore <-
  CohortGenerator::createSubsetCohortWindow(startDay = -365,
                                            endDay = -1,
                                            targetAnchor = "cohortStart")
# on-----
cohortSubsetWindowStartOnDay <-
  CohortGenerator::createSubsetCohortWindow(startDay = 0,
                                            endDay = 0,
                                            targetAnchor = "cohortStart")
# on or after-----
cohortSubsetWindowStartOnDayToAnyTimeAfter <-
  CohortGenerator::createSubsetCohortWindow(startDay = 0,
                                            endDay = 9999,
                                            targetAnchor = "cohortStart")
# after-----
cohortSubsetWindowStartOnOrAfter <-
  CohortGenerator::createSubsetCohortWindow(startDay = 1,
                                            endDay = 9999,
                                            targetAnchor = "cohortStart")
# on or after 365 ----
cohortSubsetWindowStartOnDayTo365After <-
  CohortGenerator::createSubsetCohortWindow(startDay = 0,
                                            endDay = 365,
                                            targetAnchor = "cohortStart")
# after 365-----
cohortSubsetWindowStart365AfterToAnyTimeAfter <-
  CohortGenerator::createSubsetCohortWindow(startDay = 365,
                                            endDay = 9999,
                                            targetAnchor = "cohortStart")
# after-----
cohortSubsetWindowStartAfterToAnyTimeAfter <-
  CohortGenerator::createSubsetCohortWindow(startDay = 1,
                                            endDay = 9999,
                                            targetAnchor = "cohortStart")
# after to 365 after-----
cohortSubsetWindowStartAfterTor365After <-
  CohortGenerator::createSubsetCohortWindow(startDay = 1,
                                            endDay = 365,
                                            targetAnchor = "cohortStart")