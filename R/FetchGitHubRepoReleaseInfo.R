
# Function to Fetch Tags, Version Numbers, and Release Dates
#' @export
fetchGitHubRepoReleaseInfo <- function(repoOwner, repoName) {
  # Fetch release data from GitHub API
  releaseData <-
    gh::gh("GET /repos/:owner/:repo/releases",
           owner = repoOwner,
           repo = repoName)
  
  # Initialize empty data frame
  repoInfo <- dplyr::tibble(
    tag = character(0),
    version = character(0),
    releaseDate = character(0),
    stringsAsFactors = FALSE
  )
  
  # Loop through each release
  for (i in 1:length(releaseData)) {
    tag <- releaseData[[i]]$tag_name
    version <-
      ifelse(is.null(releaseData[[i]]$name), NA, releaseData[[i]]$name)
    releaseDate <- releaseData[[i]]$created_at |> as.Date()
    
    # Append to data frame
    repoInfo <-
      rbind(repoInfo,
            dplyr::tibble(
              tag = tag,
              version = version,
              releaseDate = releaseDate
            ))
  }
  
  return(repoInfo)
}
