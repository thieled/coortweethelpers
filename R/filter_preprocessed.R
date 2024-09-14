
#' Filter Tweets Based on Referenced Types
#'
#' This function filters out observations from the `tweets` data.table in a list object based on the type of references specified. It drops rows in `tweets` where the `tweet_id` matches those in the `referenced` data.table with the specified types (e.g., "retweeted" or "quoted").
#'
#' @param preprocessed A list containing two data.tables: `tweets` and `referenced`. The `tweets` data.table should contain the data to be filtered. The `referenced` data.table should contain a `tweet_id` column and a `type` column indicating the type of reference.
#' @param drop_type A character vector specifying the types of references to drop from the `tweets` data.table. Default is `c("retweeted", "quoted")`.
#' @param filter_all_dts Logical. Should the other data.tables `referenced`, `urls`, `mentions`, and `hashtags` be filtered by remaining `tweet_id`?
#'
#' @return A list containing the modified `tweets` data.table with rows removed based on the specified `drop_type`. The `referenced` data.table remains unchanged.
#'
#' @export
#'
filter_tweets <- function(preprocessed,
                          drop_type = c("retweeted", "quoted"),
                          filter_all_dts = TRUE) {

  type <- NULL

  # Don't set keys as ids in referenced dt are not unique
  data.table::setkey(preprocessed$tweets, tweet_id)

  # Filter to get tweet_ids where type is drop_type
  retweeted_ids <- preprocessed$referenced[type %in% drop_type, tweet_id]

  # Drop rows in tweets where tweet_id is in retweeted_ids
  tweets_filtered <- preprocessed$tweets[!retweeted_ids]

  # Update the 'tweets' data.table
  preprocessed$tweets <- tweets_filtered

  # If filtering all data.tables in the list
  if (filter_all_dts) {
    # Get the remaining tweet_ids
    remaining_ids <- preprocessed$tweets[["tweet_id"]]

    # Filter other data.tables by remaining tweet_ids
    preprocessed$referenced <- preprocessed$referenced[tweet_id %in% remaining_ids]
    preprocessed$urls <- preprocessed$urls[tweet_id %in% remaining_ids]
    preprocessed$mentions <- preprocessed$mentions[tweet_id %in% remaining_ids]
    preprocessed$hashtags <- preprocessed$hashtags[tweet_id %in% remaining_ids]
  }

  return(preprocessed)
}




#' Load, Filter, and Save Tweets Data batchwise
#'
#' This function loads a list of data files, filters the 'tweets' data.table in each file based on the specified types, and saves the filtered data back to the disk. The function can also filter other data.tables in the list based on the remaining 'tweet_id's in 'tweets'.
#'
#' @param file_paths A character vector of file paths to the .rds files containing the data.tables to be processed.
#' @param save_path A character string specifying the directory path where the filtered .rds files will be saved.
#' @param drop_type A character vector indicating the types of tweets to filter out. Default is \code{c("retweeted", "quoted")}.
#' @param filter_all_dts Logical value indicating whether to filter all data.tables in the list based on the remaining 'tweet_id's in 'tweets'. Default is \code{TRUE}.
#' @param return Logical value indicating whether to return the filtered data.tables as a list. Default is \code{TRUE}.
#'
#' @return If \code{return = TRUE}, a list of filtered data.tables is returned. If \code{return = FALSE}, the function only saves the filtered data to the specified \code{save_path}.
#'
#' @export
filter_batchwise <- function(file_paths,
                             save_path,
                             drop_type = c("retweeted", "quoted"),
                             filter_all_dts = T,
                             return = T){

  # process one
  load_filter_save_one <- function(file_path,
                                   save_path,
                                   drop_type = c("retweeted", "quoted"),
                                   filter_all_dts = T){

    # Load the file
    preprocessed <- readRDS(file_path)

    # Filter
    filtered <- filter_tweets(preprocessed = preprocessed, drop_type = drop_type, filter_all_dts = filter_all_dts)

    # Set filename
    filename <- gsub("\\.rds", "_filtered.rds", basename(file_path))
    filepath <- file.path(save_path, filename)

    # Save
    saveRDS(object = filtered, file = filepath)

    # return
    if(return) return(filtered)

  }

  # rocess each file
  result_list <-  pbapply::pblapply(file_paths, function(file) {
    # Log the current file being processed
    cli::cli_inform("Processing file: {file}")

    load_filter_save_one(file, save_path = save_path, drop_type = drop_type, filter_all_dts = filter_all_dts)
  }
  )

  if(return) return(result_list)

}
