#' Detect Groups Batchwise
#'
#' This function detects groups batchwise from processed files.
#'
#' @param proc_files A character vector of file paths to processed files.
#' @param intent A character vector specifying the types of content to consider for group detection.
#'   Default is c("retweets", "hashtags", "urls", "urls_domains", "cotweet").
#' @param time_windows A numeric vector specifying the time windows (in seconds) for group detection.
#' @param save_dir A character string specifying the directory to save the resulting group files.
#' @param min_participation An integer specifying the minimum participation threshold for group detection.
#'
#' @return No return value. The detected groups are saved as RDS files in the specified directory.
#'
#' @export

detect_groups_batchwise <- function(proc_files,
                                    intent = c("retweets", "hashtags", "urls", "urls_domains", "cotweet"),
                                    time_windows,
                                    save_dir,
                                    min_participation) {
  # Initialize result list
  res <- list()

  # Iterate over each processed file
  purrr::walk(seq_along(proc_files), function(i) {
    # Read processed file
    message(paste0("Reading ", proc_files[[i]]))
    processed <- readRDS(proc_files[[i]])

    purrr::walk(seq_along(intent), function(coor_int) {

      coor_intent <- intent[[coor_int]]

      # Reshape for co-tweets
      message(paste0("Reshaping file ", basename(proc_files[[i]])), " for detection of ", coor_intent)
      coor_tweets <- CooRTweet::reshape_tweets(processed, intent = coor_intent)

      # Iterate over each time window
      purrr::walk(time_windows, function(time_window) {
        message(paste0("Detecting groups for tw ", time_window, " in ", basename(proc_files[[i]])))
        tictoc::tic()
        groups <- CooRTweet::detect_groups(coor_tweets,
                                           time_window = time_window,
                                           min_participation = min_participation)
        tictoc::toc()

        # Create filename
        filename <- basename(proc_files[[i]])
        filename <- sub("_preproc", paste0("_", coor_intent, "_tw", time_window, "_mp", min_participation), filename)
        save_path <- file.path(save_dir, filename)

        # Write to file
        message(paste0("Saving groups file to ", save_path))
        saveRDS(groups, save_path)
      })
    })
  })
}

