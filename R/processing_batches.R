#' Load Tweets from Multiple JSON Files
#'
#' Load tweets from multiple JSON files located in the specified directory.
#'
#' @param data_dir Directory containing the JSON files.
#' @param ... Additional arguments to pass to \code{\link{load_tweets_jsonl}} function.
#' @return A data.table containing tweets from all JSON files.
#' @export
load_tweets_jsonl_multi <- function(data_dir, ...) {

  json_files <- list.files(path = data_dir, pattern = "\\.(json|jsonl)$", full.names = TRUE)

  suppressWarnings(
    result <- data.table::rbindlist(
      pbapply::pblapply(json_files, function(file) {
        load_tweets_jsonl(file, ...)
      }),
      use.names = TRUE,
      fill = TRUE)
  )

  return(result)
}


#' Load User Data from Multiple JSON Files
#'
#' Load user data from multiple JSON files located in the specified directory.
#'
#' @param data_dir Directory containing the JSON files.
#' @param ... Additional arguments to pass to \code{\link{load_tweets_jsonl}} function.
#' @return A data.table containing tweets from all JSON files.
#' @export
load_users_jsonl_multi <- function(data_dir, ...) {

  json_files <- list.files(path = data_dir, pattern = "\\.(json|jsonl)$", full.names = TRUE)

  suppressWarnings(
    result <- data.table::rbindlist(
      pbapply::pblapply(json_files, function(file) {
        load_users_jsonl(file, ...)
      }),
      use.names = TRUE,
      fill = TRUE)
  )

  return(result)
}





#' This function loads tweets from JSONL files and preprocesses them.
#'
#' @param data_dir A character string specifying the directory containing JSONL files. Defaults to \code{NULL}.
#' @param file_paths A character vector specifying the paths of JSONL files. Defaults to \code{NULL}.
#' @param save_dir A character string specifying the directory to save preprocessed data. Defaults to \code{NULL}.
#' @param bind Logical. Should results be bound to one list of data tables or returned as list of lists? Default is TRUE.
#' @param api_version A character vector specifying the API version. Default is v2.
#' @param ... Additional arguments to be passed to \code{\link[jsonlite]{fromJSON}} when loading JSONL files.
#'
#' @return A list containing preprocessed tweet data.
#' @export
#'
load_preprocess_tweets_jsonl_multi <- function(data_dir = NULL,
                                               file_paths = NULL,
                                               save_dir = NULL,
                                               bind = TRUE,
                                               api_version = "v2",
                                               ...) {

  if (!is.null(data_dir)) {
    if (!dir.exists(data_dir)) {
      stop(paste0("Please provide a valid path 'data_dir'."))
    }
    json_files <- list.files(path = data_dir, pattern = "\\.(json|jsonl)$", full.names = TRUE)
  } else {
    if (is.null(file_paths)) {
      stop(paste0("Please provide either 'data_dir' or 'file_paths'."))
    }
    if (!all(file.exists(file_paths))) {
      warning(paste0("The following files do not exist and will be skipped:",
                     file_paths[!file.exists(file_paths)]
      ))
      json_files <- file_paths[file.exists(file_paths)]
    } else {
      json_files <- file_paths
    }
  }

  # Check if API version is correctly provided
  if (length(api_version) > 1) {
    stop("Please specify if 'api_version' is 'v1' or 'v2'.\n")
  }

  # Safely process each file using tryCatch to handle errors
  result_list <- pbapply::pblapply(json_files, function(file) {
    tryCatch({
      # Log the current file being processed
      cli::cli_inform("Processing file: {file}")

      # Load and preprocess the tweets file
      parsed <- load_tweets_jsonl(file, api_version = api_version, ...)
      preprocessed <- preprocess_line_tweets(parsed, api_version = api_version)

      # Store the preprocessed result if save_dir is provided
      if (!is.null(save_dir)) {
        if (!dir.exists(save_dir)) {
          stop(paste0("No such directory as ", save_dir))
        }

        if (!endsWith(save_dir, "/")) {
          save_dir <- paste0(save_dir, "/")
        }

        filepath <- paste0(save_dir, tools::file_path_sans_ext(basename(file)), "_preproc.rds")
        saveRDS(object = preprocessed, file = filepath)
      }

      # Return the preprocessed result
      return(preprocessed)

    }, error = function(e) {
      # Catch and log any errors, continue to next file
      cli::cli_alert_danger(paste0("Failed to process file: ", file, "\nError: ", e$message))
      return(NULL)  # Return NULL for failed files
    })
  })

  # Filter out any NULL results (files that failed to process)
  result_list <- Filter(Negate(is.null), result_list)

  # If bind is TRUE, combine the results into one list of data tables
  if (bind) {
    result <- rbind_preprocessed_dts(result_list)
    return(result)
  } else {
    return(result_list)
  }

}










#' This function loads user data from JSONL files and preprocesses them.
#'
#' @param data_dir A character string specifying the directory containing JSONL files. Defaults to \code{NULL}.
#' @param file_paths A character vector specifying the paths of JSONL files. Defaults to \code{NULL}.
#' @param save_dir A character string specifying the directory to save preprocessed data. Defaults to \code{NULL}.
#' @param bind Logical. Should results be bound to one list of data tables or returned as list of lists? Default is TRUE.
#' @param api_version A character vector specifying the API version. Default is v2.
#' @param ... Additional arguments to be passed to \code{\link[jsonlite]{fromJSON}} when loading JSONL files.
#'
#' @return A list containing preprocessed tweet data.
#' @export
#'
load_preprocess_users_jsonl_multi <- function(data_dir = NULL,
                                               file_paths = NULL,
                                               save_dir = NULL,
                                               bind = TRUE,
                                               api_version = "v2",
                                               ...) {

  if (!is.null(data_dir)) {
    if (!dir.exists(data_dir)) {
      stop(paste0("Please provide a valid path 'data_dir'."))
    }
    json_files <- list.files(path = data_dir, pattern = "\\.(json|jsonl)$", full.names = TRUE)
  } else {
    if (is.null(file_paths)) {
      stop(paste0("Please provide either 'data_dir' or 'file_paths'."))
    }
    if (!all(file.exists(file_paths))) {
      warning(paste0("The following files do not exist and will be skipped:",
                     file_paths[!file.exists(file_paths)]
      ))
      json_files <- file_paths[file.exists(file_paths)]
    } else {
      json_files <- file_paths
    }
  }

  # Check if API version is correctly provided
  if (length(api_version) > 1) {
    stop("Please specify if 'api_version' is 'v1' or 'v2'.\n")
  }

  # Safely process each file using tryCatch to handle errors
  result_list <- pbapply::pblapply(json_files, function(file) {
    tryCatch({
      # Log the current file being processed
      cli::cli_inform("Processing file: {file}")

      # Load and preprocess the tweets file
      parsed <- load_users_jsonl(file, api_version = api_version, ...)
      preprocessed <- preprocess_line_users(parsed, api_version = api_version)

      # Store the preprocessed result if save_dir is provided
      if (!is.null(save_dir)) {
        if (!dir.exists(save_dir)) {
          stop(paste0("No such directory as ", save_dir))
        }

        if (!endsWith(save_dir, "/")) {
          save_dir <- paste0(save_dir, "/")
        }

        filepath <- paste0(save_dir, tools::file_path_sans_ext(basename(file)), "_users_preproc.rds")
        saveRDS(object = preprocessed, file = filepath)
      }

      # Return the preprocessed result
      return(preprocessed)

    }, error = function(e) {
      # Catch and log any errors, continue to next file
      cli::cli_alert_danger(paste0("Failed to process file: ", file, "\nError: ", e$message))
      return(NULL)  # Return NULL for failed files
    })
  })

  # Filter out any NULL results (files that failed to process)
  result_list <- Filter(Negate(is.null), result_list)

  # If bind is TRUE, combine the results into one list of data tables
  if (bind) {
    result <- rbind_preprocessed_dts(result_list)
    return(result)
  } else {
    return(result_list)
  }
}

