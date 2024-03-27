#' Preprocess Line Tweets
#'
#' This function preprocesses tweet data, extracting relevant information such as entities, URLs, mentions, and hashtags, and organizes them into separate data.tables.
#'
#' @param tweets A data.table containing tweet data.
#' @param tweets_cols A character vector specifying the columns to keep in the processed data. Default columns include possibly_sensitive, lang, text, and various public metrics related to retweets, replies, likes, and quotes.
#' @return A list containing data.tables for tweets, referenced tweets, URLs, mentions, and hashtags.
#' @details This function preprocesses tweet data, ensuring that required columns such as entities, tweet_id, created_at, author_id, conversation_id, and referenced_tweets are present. It then constructs the main data.table containing all tweets and their metadata. Public metrics are handled separately, and datetime columns are reformatted. Referenced tweets, entities, URLs, mentions, and hashtags are extracted and organized into separate data.tables for easy analysis.
#'
#' @import data.table
#'
#' @export
preprocess_line_tweets <- function(tweets, tweets_cols = c(
  "possibly_sensitive", "lang", "text",
  "public_metrics_retweet_count",
  "public_metrics_reply_count",
  "public_metrics_like_count",
  "public_metrics_quote_count"
)) {
  tweet_id <- author_id <- created_at <- created_timestamp <-
    referenced_tweets <- referenced_tweet_id <- entities_urls <-
    domain <- expanded_url <- entities_mentions <- username <-
    id <- entities_hashtags <- tag <- NULL
  if (!inherits(tweets, "data.table")) {
    tweets <- data.table::as.data.table(tweets)
  }

  required_cols <- c(
    "entities",
    "tweet_id",
    "created_at",
    "author_id",
    "conversation_id",
    "referenced_tweets"
  )

  for (cname in required_cols) {
    if (!cname %in% colnames(tweets)) {
      stop("Columns or their names are incorrect.
            Ensure your data has the columns:
            entities, tweet_id, created_at")
    }
  }

  tweets_cols <- c(required_cols, tweets_cols)

  if ("public_metrics" %in% colnames(tweets)) {
    tweets[, public_metrics := purrr::map(tweets[, public_metrics], ~ unlist(.x))]
    tweets <- tidytable::unnest_wider(tweets,
                                      public_metrics,
                                      names_sep = "_",
                                      names_repair = "minimal")
  } else {
    tweets_cols <- tweets_cols[!startsWith(tweets_cols, "public_metrics")]
  }

  # Construct the main data.table containing all tweets and their meta-data
  # implicitly dropped columns: "edit_history_tweet_ids", "withheld"
  tmp_keep_cols <- colnames(tweets)[colnames(tweets) %in% tweets_cols]

  Tweets <- tweets[, tmp_keep_cols, with = FALSE]
  data.table::setindex(Tweets, tweet_id, author_id)

  # reformat datetime of created_at
  Tweets[, created_at := lubridate::as_datetime(created_at, tz = "UTC")]
  Tweets[, created_timestamp := as.numeric(created_at)]

  # Get data.table of referenced tweets
  # Filter rows with non-NULL referenced_tweets
  Referenced <- tweets[purrr::map_lgl(tweets$referenced_tweets, ~!is.null(.x)), .(tweet_id, referenced_tweets)]
  # Make "referenced_tweets" column a vector that can be processed by unnest_wider
  Referenced[, referenced_tweets := purrr::map(Referenced[, referenced_tweets], ~ unlist(.x))]
  # Unnest wider
  Referenced <- tidytable::unnest_wider(Referenced, referenced_tweets, names_sep = "_", names_repair = "unique_quiet")
  Referenced <- Referenced[, 1:3, with = FALSE] # bit of a hack but works

  data.table::setnames(
    Referenced,
    c("tweet_id", "type", "referenced_tweet_id")
  )

  data.table::setindex(Referenced, tweet_id, referenced_tweet_id)

  # unnest "entities", contains: urls, hashtags, other users
  # Note: The CooRtweet:::unnest_wider did not work here, so I'm using my own a tailored function
  entities <- extract_entities_dt(tweets)

  # Extract the data.table holding all URLs
  # Note: I replace the "expanded_url" by the "unwound_url" if this is available (points to original source if URL was shortened, e.g. bit.ly)
  URLs <- extract_URL_dt(entities)

  # Extract mentions
  Mentions <- extract_mentions_dt(entities)

  # # Construct data.table with all hashtags
  Hashtags <- extract_hashtags_dt(entities)

  # Free memory
  rm(entities, tweets)
  gc()

  return(list(
    tweets = Tweets,
    referenced = Referenced,
    urls = URLs,
    mentions = Mentions,
    hashtags = Hashtags
  ))
}









#' Extract Entities Data.Table from Twitter Data
#'
#' This function extracts entities information from Twitter data and organizes it into a data.table in wide format.
#'
#' @param tweets A data.table containing Twitter data, including tweet_id and entities columns.
#' @return A data.table containing entities information extracted from Twitter data in wide format.
#' @details This function drops empty rows, subsets the data.table to tweet_id and entities columns, unnests the entities column into a longer format,
#' and pivots the data into wide format. It creates a unique row identifier for each tweet_id - entities_id pair to ensure uniqueness in the resulting data.table.
#'
#' @import data.table
#'
#' @export
extract_entities_dt <- function(tweets) {
  ## For some reason, CooRTweet:::unnest_wider does not work properly here - different solution
  # Note: There is probably a more efficient way to achieve this

  # Drop empty rows, subset data.table to tweet_id and entities column
  ent_dt <- tweets[purrr::map_lgl(tweets$entities, ~!is.null(.x)), .(tweet_id, entities)]

  # make entities column a vector
  ent_dt[, entities := purrr::map(ent_dt[, entities], ~ unlist(.x))]

  # unnest into longer format
  ent_dt <- tidytable::unnest_longer(ent_dt, entities, names_repair = "minimal")

  # Turn into data.table
  data.table::setDT(ent_dt)

  # Create a unique row identifier for each tweet_id - entities_id pair
  ent_dt[, row_id := seq_len(.N), by = .(tweet_id, entities_id)]

  # Pivot the data to wide format
  ent_dt <- data.table::dcast(ent_dt, tweet_id + row_id ~ entities_id, value.var = "entities")

  return(ent_dt)
}







#' Extract URLs Data.Table from Entities Data.Table
#'
#' This function extracts URLs shared in tweets from the entities data.table and organizes them into a separate data.table.
#'
#' @param entities A data.table containing entities information extracted from Twitter data, typically obtained using the extract_entities_dt function.
#' @return A data.table containing URLs shared in the tweets, along with related information such as tweet_id, row_id, URL details, and domain names.
#' @details This function evaluates if there are tweets with several URLs, resulting in a urls.url column. It then subsets the data into tweets
#' containing one URL and tweets with several URLs. Column names starting with "url." are harmonized, and the prefix "urls." is removed.
#' Empty observations are dropped, and a new column clear_url is created using data.table syntax. Domain names are extracted from the expanded_url column and stored in the domain column.
#' The resulting data.table is indexed by tweet_id, domain, and expanded_url for efficient querying.
#'
#' @import data.table
#'
#' @export
extract_URL_dt <- function(entities){

  # Define url columns
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

    URLs <- entities[, url_cols, with = FALSE]

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






#' Extract Mentions Data.Table from Entities Data.Table
#'
#' This function extracts mentions from tweets' entities data.table and organizes them into a separate data.table.
#'
#' @param entities A data.table containing entities information extracted from Twitter data, typically obtained using the extract_entities_dt function.
#' @return A data.table containing mentions extracted from the tweets, along with related information such as tweet_id, row_id, username, id, start, and end.
#' @details This function evaluates if there are tweets with several mentions, resulting in a mentions.id column. It then subsets the data into tweets containing
#' one mention and tweets with several mentions. Column names starting with "mentions" are harmonized, and the prefix "mentions." is removed. Empty observations are dropped,
#' and the resulting data.table is indexed by tweet_id, username, and id for efficient querying.
#'
#' @import data.table
#'
#' @export
extract_mentions_dt <- function(entities) {
  # Define mention columns
  mentions_cols <- c("tweet_id",
                     "row_id",
                     "username",
                     "id",
                     "start",
                     "end")

  # Evaluate if there are tweets with several mentions, resulting in a mentions.id column
  if ("mentions.id" %in% colnames(entities)) {
    multi_mentions_cols <- c(
      "tweet_id",
      "row_id",
      "mentions.username",
      "mentions.id",
      "mentions.start",
      "mentions.end"
    )

    # Subset into tweets containing one mention and tweets with several mentions
    one_mentions <- entities[, mentions_cols, with = FALSE]
    multi_mentions <- entities[, multi_mentions_cols, with = FALSE]

    # Harmonize the column names
    # Get column names starting with "mentions"
    m_mentions_cols <- grep("^mentions\\.", names(multi_mentions), value = TRUE)

    # Remove the prefix "mentions."
    new_names <- gsub("^mentions\\.", "", m_mentions_cols)

    # Rename the columns
    data.table::setnames(multi_mentions, m_mentions_cols, new_names)

    # Bind the two data.tables
    Mentions <- data.table::rbindlist(list(one_mentions, multi_mentions))
  } else {
    Mentions <- entities[, mentions_cols, with = FALSE]
  }

  # Drop empty observations
  Mentions <- Mentions[purrr::map_lgl(Mentions$username, ~!is.na(.x))]

  # Set index for efficient querying
  data.table::setindex(Mentions, tweet_id, username, id)

  return(Mentions)
}




#' Extract Hashtags Data.Table from Entities Data.Table
#'
#' This function extracts hashtags from tweets' entities data.table and organizes them into a separate data.table.
#'
#' @param entities A data.table containing entities information extracted from Twitter data, typically obtained using the extract_entities_dt function.
#' @return A data.table containing hashtags extracted from the tweets, along with related information such as tweet_id, row_id, tag, start, and end.
#' @details This function evaluates if there are tweets with several hashtags, resulting in a hashtags.tag column. It then subsets the data into tweets
#' containing one hashtag and tweets with several hashtags. Column names starting with "hashtags" are harmonized, and the prefix "hashtags" is removed.
#' Empty observations are dropped, and the resulting data.table is indexed by tweet_id and tag for efficient querying.
#'
#' @import data.table
#'
#' @export
extract_hashtags_dt <- function(entities) {
  # Define hashtag columns
  hashtags_cols <- c("tweet_id",
                     "row_id",
                     "tag", "start", "end")

  # Evaluate if there are tweets with several hashtags, resulting in a hashtags.tag column
  if ("hashtags.tag" %in% colnames(entities)) {
    multi_hashtags_cols <- c(
      "tweet_id",
      "row_id",
      "hashtags.tag",
      "hashtags.start",
      "hashtags.end"
    )

    # Subset into tweets containing one hashtag and tweets with several hashtags
    one_hashtags <- entities[, hashtags_cols, with = FALSE]
    multi_hashtags <- entities[, multi_hashtags_cols, with = FALSE]

    # Harmonize the column names
    # Get column names starting with "hashtags"
    m_hashtags_cols <- grep("^hashtags\\.", names(multi_hashtags), value = TRUE)

    # Remove the prefix "hashtags"
    new_names <- gsub("^hashtags\\.", "", m_hashtags_cols)

    # Rename the columns
    data.table::setnames(multi_hashtags, m_hashtags_cols, new_names)

    # Bind the two data.tables
    Hashtags <- data.table::rbindlist(list(one_hashtags, multi_hashtags))
  } else {
    Hashtags <- entities[, hashtags_cols, with = FALSE]
  }

  # Drop empty observations
  Hashtags <- Hashtags[purrr::map_lgl(Hashtags$tag, ~!is.na(.x))]

  # Set index for efficient querying
  data.table::setindex(Hashtags, tweet_id, tag)

  return(Hashtags)
}





#' Row-bind Preprocessed Data Tables
#'
#' Combine preprocessed data.tables stored in a nested list into a list of row-binded data.tables,
#' deduplicating them by tweet_id.
#'
#' @param dt_list A nested list containing preprocessed data.tables.
#' @return A list containing row-binded data.tables with deduplication by
#' tweet_id for each type of data.
#' @export
rbind_preprocessed_dts <- function(dt_list) {
  # Extract table names from the first list
  table_names <- names(dt_list[[1]])

  # Row-bind all tables for each table type
  row_binded_tables <- lapply(table_names, function(name) {
    tables <- lapply(dt_list, function(x) x[[name]])
    dt <- data.table::rbindlist(tables, fill = TRUE)
    dt <- unique(dt, by = "tweet_id")  # Deduplicate by tweet_id
    return(dt)
  })

  # Name the elements of the resulting list with the table names
  names(row_binded_tables) <- table_names

  return(row_binded_tables)
}

