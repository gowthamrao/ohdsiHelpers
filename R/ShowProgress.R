showProgress <-
  function(currentIteration,
           totalIterations,
           extraMessage = NULL) {
    progress <- (currentIteration / totalIterations) * 100
    message <-
      sprintf(
        "\rProgress: %d/%d (%0.2f%%)",
        currentIteration,
        totalIterations,
        progress
      )

    if (!is.null(extraMessage)) {
      message <- paste0(message, ". ", extraMessage)
    }
    cat(message)
    flush.console()
  }
