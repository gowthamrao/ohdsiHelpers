#' @export
queryChatGpt <- function(prompt,
                         apiKey = Sys.getenv("chatGptApiKey"),
                         model = "gpt-3.5-turbo") {
  chatGptAnswer <- httr::POST(
    url = "https://api.openai.com/v1/chat/completions",
    httr::add_headers(Authorization = paste("Bearer", apiKey)),
    httr::content_type_json(),
    encode = "json",
    body = list(
      model = model,
      messages = list(list(
        role = "user",
        content = prompt
      ))
    )
  )

  (stringr::str_trim(httr::content(chatGptAnswer)$choices[[1]]$message$content))
}
