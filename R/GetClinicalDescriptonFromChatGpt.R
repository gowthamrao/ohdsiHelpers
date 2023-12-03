#' @export
getClinicalDescriptonFromChatGpt <- function(condition,
                                             apiKey = Sys.getenv("chatGptApiKey"),
                                             generatedDate = Sys.Date(),
                                             model = "gpt-3.5-turbo",
                                             disclaimer = paste0(
                                               "###Disclaimer: Generated using ChatGPT on ",
                                               generatedDate,
                                               " using model ",
                                               model,
                                               ". Please use caution."
                                             )) {
  prompt <-
    paste0(
      "Please compose a detailed report on the condition: '",
      condition,
      "'. Aimed at medical professionals, you should include technical terms related to general pathophysiology, but avoid delving into molecular biology. The reader is assumed to be a general practitioner or specialist physician without a background in research. Use Vancouver style for inline citations, ordering them by their first appearance in the text. Ensure every statement is backed by a reference from a publication indexed on the National Library Of Medicine's PubMed. Do not use double quotes or non utf-8 characters in the output. The output should be in plain text and formatted using markdown. Use the following template. The portions in square brackets [] are what I want you to develop as an expert.

#Condition: [Name of condition mentioned above.]

##Overview: [Write a concise description of the condition. ]

##Synonyms: [List other names for the condition.]

##Presentation: [Detail the signs and symptoms of condition.]

##Diagnostics Evaluation: [Describe the assessment criteria for Condition. Include suitable tests and potential results where relevant.]

##Differential diagnoses: [Provide a list of conditions that could potentially be mistaken with the condition.]

##Treatment plan: [Outline the standard treatment plan for condition, using bullet points. If multiple treatment options exist, treat each as a separate bullet point. Indicate whether the treatment plan is the current or a previous standard that has since evolved.]

##Prognosis: [Write a brief paragraph on the prognosis for a patient diagnosed with Condition, distinguishing between short term (within the first 3 months after diagnosis) and long term (1 year or more after diagnosis) prognoses.]

##Exclusions: [List any conditions or treatments that must be ruled out at the time of diagnosis. ]

#Ambiguity:[Identify any conditions with similar names to condition but that represent different clinical ideas. Do not include subtypes of condition.]

##Subtypes: [Identify any subtypes of condition.]

##References: [Include at least 10 references that were used in creating this report, ensuring they are indexed on PubMed and compatible with a reference manager.]
  "
    )

  output <- queryChatGpt(
    prompt = prompt,
    apiKey = apiKey,
    model = model
  )

  if (length(disclaimer) > 0) {
    output <- paste0(output, "\n\n", disclaimer)
  }

  return(output)
}
