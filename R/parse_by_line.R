#' Load Tweets from JSON or JSONL File
#'
#' This function parses and reshapes .json and .jsonl data from the Twitter v1 and v2 APIs into a format that
#' can be processed by the CooRTweet package.
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
#' @param api_version A character value indicating whether data from Twitter API 'v1' or 'v2' should be parsed. Default is v2.
#' @param parser A character indicating which json parser should be used. Default "simdjson"; otherwise "yyjson".
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
    query = NULL,
    query_error_ok = TRUE,
    on_query_error = NULL,
    parse_error_ok = TRUE,
    on_parse_error = NULL,
    empty_array = NULL,
    empty_object = NULL,
    max_simplify_lvl = 2L,
    api_version = "v2",
    parser = "simdjson"
) {

  # Check if api version is correctly provided
  if (length(api_version) > 1) {
    stop("Please specify if 'api_version' is 'v1' or 'v2'.\n")
  }

  # If v2 API, query for /data in jsons
  if(api_version == "v2"){
    query = "/data"
  }

  # Pick number of threads if not specified
  if(is.null(num_threads)){
    num_threads <- parallel::detectCores()
  }

  # Parse one json file line-by-line
  # Note: This is necessary if RcppSimdJson::fload/fparse breaks on the whole file due to corrupt lines
  # and (I think) for parsing .jsonl files.
  if(parser == "simdjson"){

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
  }else{

    jsonlines <- readr::read_lines(file = file_path,
                                   num_threads = num_threads,
                                   n_max = n_max,
                                   skip_empty_rows = skip_empty_rows)

    jsonl <- lapply(jsonlines, function(x) magrittr::extract2(yyjsonr::read_json_str(x), "data"))

  }
  # For v2: Check if parsed jsons are further nested into pages, if so, unlist
  # To economize, inspect only the first 200 elements
  if(api_version == "v2"){

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
  }

  # For data.table::rbindlist, 'simple' lists must be restructured into more nested lists..
  # TO DO: Find a way to skip this step
  jsonl <- purrr::map(jsonl, ~ purrr::modify(.x, ~ if (is.list(.x) && length(.x) > 1) list(.x) else .x))

  dt <- data.table::rbindlist(jsonl,
                              use.names = TRUE,
                              fill = TRUE)

  # rename "id" column
  if(api_version == "v2"){
    data.table::setnames(dt, "id", "tweet_id")
  }else{
    data.table::setnames(dt, "id_str", "tweet_id")
  }

  # deduplicate
  dt <- unique(dt, by = "tweet_id")

  data.table::alloc.col(dt)

  return(dt)
}



#' Load Users from JSONL File
#'
#' This function loads user data from a JSONL (JSON Lines) file and extracts relevant fields using the RcppSimdJson package.
#' It handles both v1 and v2 of the API by adjusting the query accordingly and flattens the resulting data.
#'
#' @param file_path A string specifying the path to the JSONL file.
#' @param num_threads An integer specifying the number of threads to use. If `NULL`, it detects the number of cores automatically.
#' @param n_max Maximum number of lines to read from the file. Defaults to `Inf` (no limit).
#' @param skip_empty_rows Logical, whether to skip empty rows when reading the file. Defaults to `TRUE`.
#' @param query A character vector specifying the JSONPath to extract specific data. If `NULL`, it defaults based on the API version.
#' @param query_error_ok Logical, whether to ignore errors during query processing. Defaults to `TRUE`.
#' @param on_query_error A function or `NULL`, specifying the handler for query errors. Defaults to `NULL`.
#' @param parse_error_ok Logical, whether to ignore errors during parsing. Defaults to `TRUE`.
#' @param on_parse_error A function or `NULL`, specifying the handler for parsing errors. Defaults to `NULL`.
#' @param empty_array A value or `NULL`, specifying what to return for empty arrays. Defaults to `NULL`.
#' @param empty_object A value or `NULL`, specifying what to return for empty objects. Defaults to `NULL`.
#' @param max_simplify_lvl An integer specifying the maximum simplification level for the parsed JSON. Defaults to `2L`.
#' @param api_version A string specifying the API version, either `"v1"` or `"v2"`. Defaults to `"v2"`.
#'
#' @return A data.table containing user information from the JSONL file.
#' The user ID column is renamed based on the API version, and duplicate rows are removed.
#'
#' @export
load_users_jsonl <- function(
    file_path,
    num_threads = NULL,
    n_max = Inf,
    skip_empty_rows = TRUE,
    query = NULL,
    query_error_ok = TRUE,
    on_query_error = NULL,
    parse_error_ok = TRUE,
    on_parse_error = NULL,
    empty_array = NULL,
    empty_object = NULL,
    max_simplify_lvl = 2L,
    api_version = "v2"
) {

  # Check if API version is correctly provided
  if (length(api_version) > 1) {
    stop("Please specify if 'api_version' is 'v1' or 'v2'.\n")
  }

  # Set query based on API version
  if(api_version == "v2"){
    query = "/includes/users"
  } else {
    query = c("/user", "/retweeted_status/user", "/quoted_status/user")
  }

  # Detect number of threads if not specified
  if(is.null(num_threads)){
    num_threads <- parallel::detectCores()
  }

  # Parse the JSONL file line-by-line
  jsonl <- RcppSimdJson::fparse(
    readr::read_lines(file = file_path,
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
    max_simplify_lvl = max_simplify_lvl
  ) |>
    purrr::flatten()  # Flatten the upper layer of the list

  # Restructure simple lists into more nested lists
  jsonl <- purrr::map(jsonl, ~ purrr::modify(.x, ~ if (is.list(.x) && length(.x) > 1) list(.x) else .x))

  # Combine into a data.table
  dt <- data.table::rbindlist(jsonl, use.names = TRUE, fill = TRUE)

  # Rename "id" column based on API version
  if(api_version == "v2") {
    data.table::setnames(dt, "id", "user_id")
  } else {
    data.table::setnames(dt, "id_str", "user_id")
  }

  # Remove duplicate users by user_id
  dt <- unique(dt, by = "user_id")

  return(dt)
}



