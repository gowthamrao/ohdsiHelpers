#' Communicate with a local instance of GPT-4
#'
#' This function sends a prompt to a local instance of GPT-4 and retrieves the response.
#' It supports input from direct text, a PDF file, or a data frame.
#' The function allows customization of various parameters for the GPT-4 API request.
#'
#' @param prompt (required) A character string containing the user's prompt.
#' @param systemPrompt (optional) A character string containing the system prompt to set the context or behavior of the
#'                        assistant. Default is an empty string.
#' @param additionalText (optional) A blob of lot of text. e.g. read from one or more pdf files.
#' @param dataFrame (optional) A data frame to be converted to JSON and used as the prompt. Default is NULL.
#' @param url A character string containing the URL of the GPT-4 endpoint. Retrieved from keyring by default.
#' @param apiKey A character string containing the API key for the GPT-4 endpoint. Retrieved from keyring by default.
#' @param temperature (optional) A numeric value controlling the randomness of the response. Default is 0.00000001.
#' @param frequencyPenalty (optional) A numeric value affecting the likelihood of repeating the same lines or phrases. Default is 0.
#' @param presencePenalty (optional) A numeric value affecting the likelihood of introducing new topics. Default is 0.
#' @param maxTokens (optional) An integer specifying the maximum number of tokens to generate in the response. Default is 4096.
#'
#' @return A array of objects with output from GPT-4. Use "output" for the final text output.
#' @export
getGpt4Response <- function(prompt,
                            systemPrompt = "",
                            additionalText = NULL,
                            dataFrame = NULL,
                            url = keyring::key_get("genai_gpt4_endpoint"),
                            apiKey = keyring::key_get("genai_api_gpt4_key"),
                            temperature = 0.00000001,
                            frequencyPenalty = 0,
                            presencePenalty = 0,
                            maxTokens = 4096) {
  if (is.null(prompt)) {
    stop("'prompt' must be provided.")
  }
  
  if (!is.null(additionalText)) {
    prompt <- paste(prompt, additionalText, collapse = "\n")
  }
  
  if (!is.null(dataFrame)) {
    dataFrameAsJson <- jsonlite::toJSON(dataFrame, pretty = TRUE)
    prompt <- paste(prompt, dataFrameAsJson, collapse = "\n")
  }
  
  jsonPayload <- jsonlite::toJSON(
    list(
      messages = list(
        list(role = "system", content = systemPrompt),
        list(role = "user", content = prompt)
      ),
      temperature = temperature,
      frequency_penalty = frequencyPenalty,
      presence_penalty = presencePenalty,
      max_tokens = maxTokens
    ),
    auto_unbox = TRUE
  )
  
  sendPostRequest <- function(url, apiKey, jsonPayload) {
    httr::POST(
      url = url,
      body = jsonPayload,
      httr::add_headers("Content-Type" = "application/json", "api-key" = apiKey),
      config = httr::config(http_version = 1.1)
    )
  }
  
  startTime <- Sys.time()
  response <- tryCatch({
    sendPostRequest(url, apiKey, jsonPayload)
  }, error = function(e) {
    stop("Failed to connect to the API: ", e$message)
  })
  
  delta <- Sys.time() - startTime
  message(sprintf(
    "- Generating response took %0.1f %s",
    delta,
    attr(delta, "units")
  ))
  
  result <- list()
  result$response <- response
  result$json <-
    httr::content(result$response, "text", encoding = "UTF-8")
  result$jsonParsed <- jsonlite::fromJSON(result$json)
  
  if ("choices" %in% names(result$jsonParsed) &&
      length(result$jsonParsed$choices) > 0 &&
      "message" %in% names(result$jsonParsed$choices) &&
      "content" %in% names(result$jsonParsed$choices$message)) {
    result$output <- result$jsonParsed$choices$message$content
  } else {
    result$output <- "Unexpected JSON structure or no valid output."
    message("JSON parsed: ", result$json)
  }
  return(result)
}
