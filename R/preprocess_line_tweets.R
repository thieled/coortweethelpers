
#' Extract URL Data.Table from Twitter Data
#'
#' This function extracts URL information from Twitter data and organizes it into a data.table format.
#'
#' @param tweets A data.table containing Twitter data, including tweet_id and entities columns.
#' @return A data.table containing URL information extracted from Twitter data.
#' @details This function drops empty rows, subsets the data.table to tweet_id and entities columns, unnests the entities column into a longer format, and pivots the data into wide format. If multiple URLs are present in a tweet, it separates them into individual rows and harmonizes the column names. It also drops empty observations and creates a new column "clear_url" based on the condition provided. Additionally, it extracts domain names from the expanded_url column.
#'
#' @import data.table
#'
#' @export
extract_URL_dt <- function(tweets){


  ## For some reason, CooRTweet:::unnest_wider does not work properly here - different solution
  # Note: There is probably a more efficient way to achieve this

  # Drop empty rows, subset data.table to tweet_id and entites column
  entities <- tweets[purrr::map_lgl(tweets$entities, ~!is.null(.x)), .(tweet_id, entities)]

  # make entities column a vector
  entities[, entities := purrr::map(dt_filtered[, entities], ~ unlist(.x))]

  # unnest into longer format
  entities <- tidytable::unnest_longer(entities, entities, names_repair = "minimal", )

  # Turn into data.table
  data.table::setDT(entities)

  # Create a unique row identifier for each tweet_id - entities_id pair
  entities[, row_id := seq_len(.N), by = .(tweet_id, entities_id)]

  # Pivot the data to wide format
  entities <- data.table::dcast(entities, tweet_id + row_id ~ entities_id, value.var = "entities")

  # Subset
  url_cols <- c(
    "tweet_id",
    "row_id",
    "url",
    "expanded_url",
    "display_url",
    "title",
    "description",
    "unwound_url",
    "start",
    "end"
  )

  # Evaluate if there are tweets with several urls, resulting in a urls.url column
  if ("urls.url" %in% colnames(entities)) {

    multiurl_cols <- c(
      "tweet_id",
      "row_id",
      "urls.url",
      "urls.expanded_url",
      "urls.display_url",
      "urls.title",
      "urls.description",
      "urls.unwound_url",
      "urls.start",
      "urls.end"
    )

    # Subset into tweets containing one url and tweets with several urls
    oneurl <- entities[, url_cols, with = FALSE]
    multiurl <- entities[, multiurl_cols, with = FALSE]

    # Harmonize the column names
    # Get column names starting with "url."
    m_url_cols <- grep("^urls\\.", names(multiurl), value = TRUE)

    # Remove the prefix "urls."
    new_names <- gsub("^urls\\.", "", m_url_cols)

    # Rename the columns
    data.table::setnames(multiurl, m_url_cols, new_names)

    # Bind the two data.tables
    URLs <- data.table::rbindlist(list(oneurl, multiurl))
  }else{
    URLs <- entities[, oneurl_cols, with = FALSE]
  }

  # Drop empty observations
  URLs <- URLs[purrr::map_lgl(URLs$url, ~!is.na(.x))]

  # Create the new column clear_url using data.table syntax
  URLs[, expanded_url := ifelse(!is.na(unwound_url), unwound_url, expanded_url)]

  # extract domain names
  URLs[, domain := gsub("https?://", "", expanded_url)]
  URLs[, domain := stringi::stri_split_fixed(domain, "/", n = 2, simplify = TRUE)[, 1]]

  data.table::setindex(URLs, tweet_id, domain, expanded_url)

  return(URLs)

}
















