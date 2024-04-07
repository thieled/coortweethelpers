#' Load Tweets from JSON or JSONL File
#'
#' This function parses and reshapes .json and .jsonl data from the Twitter v1 and v2 APIs into a format that can be processed by the CooRTweet package.
#'
#' @param file_path A character string specifying the path to the .json or .jsonl file.
#' @param num_threads An integer specifying the number of threads to be used for parsing. Default is NULL.
#' @param n_max An integer specifying the maximum number of lines to read from the file. Default is Inf.
#' @param skip_empty_rows A logical value indicating whether empty rows should be skipped. Default is TRUE.
#' @param query A character string specifying the JSON path to extract data from. Default is "/data".
#' @param query_error_ok A logical value indicating whether to continue parsing if a query error occurs. Default is TRUE.
#' @param on_query_error A function to handle query errors if they occur. Default is NULL.
#' @param parse_error_ok A logical value indicating whether to continue parsing if a parse error occurs. Default is TRUE.
#' @param on_parse_error A function to handle parse errors if they occur. Default is NULL.
#' @param empty_array A value to use for empty arrays encountered during parsing. Default is NULL.
#' @param empty_object A value to use for empty objects encountered during parsing. Default is NULL.
#' @param max_simplify_lvl An integer specifying the maximum level of simplification. Default is 2L (vector).
#' Other options from RcppSimdJson::fparse are data.frame (0L), matrix (1L), vector (2L) or list (3L).
#'
#' @return A data.table object containing the parsed and reshaped tweets.
#'
#' @details This function reads a .json or .jsonl file line by line to avoid potential errors with RcppSimdJson. It then processes the data into a format suitable for further analysis.
#'
#' @examples
#' \dontrun{
#'   tweets <- load_tweets_json_line("tweets.json")
#' }
#'
#' @references
#' For more information on the CooRTweet package, see: https://github.com/nicolarighetti/CoRTweet
#'
#' @export
load_tweets_jsonl <- function(
    file_path,
    num_threads = NULL,
    n_max = Inf,
    skip_empty_rows = TRUE,
    query = "/data",
    query_error_ok = TRUE,
    on_query_error = NULL,
    parse_error_ok = TRUE,
    on_parse_error = NULL,
    empty_array = NULL,
    empty_object = NULL,
    max_simplify_lvl = 2L
) {

  # Pick number of threads if not specified
  if(is.null(num_threads)){
    num_threads <- parallel::detectCores()
  }

  # Parse one json file line-by-line
  # Note: This is necessary if RcppSimdJson::fload/fparse breaks on the whole file due to corrupt lines
  # and (I think) for parsing .jsonl files.
  jsonl <- RcppSimdJson::fparse(readr::read_lines(file = file_path,   # I use readr::read_lines, which has been found to be faster than readLines or vroom::vroom_lines
                                                  num_threads = num_threads,
                                                  n_max = n_max,
                                                  skip_empty_rows = skip_empty_rows),
                                query = query,
                                query_error_ok = query_error_ok,
                                on_query_error = on_query_error,
                                parse_error_ok = parse_error_ok,
                                on_parse_error = on_parse_error,
                                empty_array = empty_array,
                                empty_object = empty_object,
                                max_simplify_lvl = max_simplify_lvl)


  # Check if parsed jsons are further nested into pages, if so, unlist
  # To economize, inspect only the first 200 elements
          determine_pagination <- function(lst) {
            list_length <- sapply(utils::head(lst, 200), length)
            list_names <- sapply(utils::head(lst, 200), names)

            if (mean(list_length) < 25 && all(!is.null(list_names))) {
              return(FALSE)
            } else {
              return(TRUE)
            }
          }

          # Conditional unlist function
          unlist_paged_list <- function(lst) {
            if (determine_pagination(lst)) {
              lst <- unlist(lst, recursive = FALSE)
            }
            return(lst)
          }

          # Apply these functions
          jsonl <- unlist_paged_list(jsonl)

  # For data.table::rbindlist, 'simple' lists must be restructured into more nested lists..
  # TO DO: Find a way to skip this step
  jsonl <- purrr::map(jsonl, ~ purrr::modify(.x, ~ if (is.list(.x) && length(.x) > 1) list(.x) else .x))

  dt <- data.table::rbindlist(jsonl,
                              use.names = TRUE,
                              fill = TRUE)

  # rename "id" column
  data.table::setnames(dt, "id", "tweet_id")

  # deduplicate
  dt <- unique(dt, by = "tweet_id")

  data.table::alloc.col(dt)

  return(dt)
}




#' Load Users from JSON or JSONL File
#'
#' This function parses and reshapes .json and .jsonl data containing user information into a format suitable for further analysis.
#'
#' @param file_path A character string specifying the path to the .json or .jsonl file.
#' @param num_threads An integer specifying the number of threads to be used for parsing. Default is NULL.
#' @param n_max An integer specifying the maximum number of lines to read from the file. Default is Inf.
#' @param skip_empty_rows A logical value indicating whether empty rows should be skipped. Default is TRUE.
#' @param query A character string specifying the JSON path to extract user data from. Default is "/includes/users".
#' @param query_error_ok A logical value indicating whether to continue parsing if a query error occurs. Default is TRUE.
#' @param on_query_error A function to handle query errors if they occur. Default is NULL.
#' @param parse_error_ok A logical value indicating whether to continue parsing if a parse error occurs. Default is TRUE.
#' @param on_parse_error A function to handle parse errors if they occur. Default is NULL.
#' @param empty_array A value to use for empty arrays encountered during parsing. Default is NULL.
#' @param empty_object A value to use for empty objects encountered during parsing. Default is NULL.
#' @param max_simplify_lvl An integer specifying the maximum level of simplification. Default is 2L (vector).
#' Other options from RcppSimdJson::fparse are data.frame (0L), matrix (1L), vector (2L) or list (3L).
#'
#' @return A data.table object containing the parsed and reshaped user data.
#'
#' @details This function reads a .json or .jsonl file line by line to avoid potential errors with RcppSimdJson. It then processes the user data into a format suitable for further analysis.
#'
#' @examples
#' \dontrun{
#'   users <- load_users_json_line("tweets.json")
#' }
#'
#' @references
#' For more information on the CooRTweet package, see: https://github.com/nicolarighetti/CoRTweet
#'
#' @export
load_users_jsonl <- function(
    file_path,
    num_threads = NULL,
    n_max = Inf,
    skip_empty_rows = TRUE,
    query = "/includes/users",
    query_error_ok = TRUE,
    on_query_error = NULL,
    parse_error_ok = TRUE,
    on_parse_error = NULL,
    empty_array = NULL,
    empty_object = NULL,
    max_simplify_lvl = 2L
) {

  # Pick number of threads if not specified
  if(is.null(num_threads)){
    num_threads <- parallel::detectCores()
  }

  # Parse one json file line-by-line
  # Note: This is necessary if RcppSimdJson::fload/fparse breaks on the whole file due to corrupt lines
  # and (I think) for parsing .jsonl files.
  jsonl <- RcppSimdJson::fparse(readr::read_lines(file = file_path,   # I use readr::read_lines, which has been found to be faster than readLines or vroom::vroom_lines
                                                  num_threads = num_threads,
                                                  n_max = n_max,
                                                  skip_empty_rows = skip_empty_rows),
                                query = query,
                                query_error_ok = query_error_ok,
                                on_query_error = on_query_error,
                                parse_error_ok = parse_error_ok,
                                on_parse_error = on_parse_error,
                                empty_array = empty_array,
                                empty_object = empty_object,
                                max_simplify_lvl = max_simplify_lvl) |>
    purrr::flatten() ## Remove upper layer of list

  # For data.table::rbindlist, 'simple' lists must be restructured into more nested lists..
  # TO DO: Find a way to skip this step
  jsonl <- purrr::map(jsonl, ~ purrr::modify(.x, ~ if (is.list(.x) && length(.x) > 1) list(.x) else .x))

  dt <- data.table::rbindlist(jsonl,
                              use.names = TRUE,
                              fill = TRUE)

  # rename "id" column
  data.table::setnames(dt, "id", "user_id")

  # deduplicate
  dt <- unique(dt, by = "user_id")

  return(dt)
}





#' Load Tweets from Multiple JSON Files
#'
#' Load tweets from multiple JSON files located in the specified directory.
#'
#' @param data_dir Directory containing the JSON files.
#' @param ... Additional arguments to pass to \code{\link{load_tweets_jsonl}} function.
#' @return A data.table containing tweets from all JSON files.
#' @export
load_tweets_jsonl_multi <- function(data_dir, ...) {
  if (!endsWith(data_dir, "/")) {
    data_dir <- paste0(data_dir, "/")
  }
  json_files <- Sys.glob(paste0(data_dir, "*.json*"))

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




#' Load and Preprocess Tweets from Multiple JSON Files
#'
#' Load and preprocess tweets from multiple JSON files located in the specified directory.
#'
#' @param data_dir Directory containing the JSON files.
#' @param ... Additional arguments to pass to \code{\link{load_tweets_jsonl}} function.
#' @return A list of data.tables containing preprocessed tweets from all JSON files.
#' @export
load_preprocess_tweets_jsonl_multi <- function(data_dir, ...) {
  if (!endsWith(data_dir, "/")) {
    data_dir <- paste0(data_dir, "/")
  }
  json_files <- Sys.glob(paste0(data_dir, "*.json*"))

  suppressWarnings(
    result_list <-  pbapply::pblapply(json_files, function(file) {
      parsed <- load_tweets_jsonl(file, ...)
      preprocessed <- preprocess_line_tweets(parsed)
    }
    )
  )
  result <- rbind_preprocessed_dts(result_list)

  return(result)
}
