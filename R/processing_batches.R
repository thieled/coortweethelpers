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
#' @param ... Additional arguments to be passed to \code{\link[jsonlite]{fromJSON}} when loading JSONL files.
#'
#' @return A list containing preprocessed tweet data.
#' @export
#'
load_preprocess_tweets_jsonl_multi <- function(data_dir = NULL,
                                               file_paths = NULL,
                                               save_dir = NULL,
                                               bind = TRUE,
                                               ...) {

  if(!is.null(data_dir)){

    if (!endsWith(data_dir, "/")) {
      data_dir <- paste0(data_dir, "/")
    }

    if(!dir.exists(data_dir)){
      stop(paste0("Please provide a valid path 'data_dir'."))
    }

    json_files <- Sys.glob(paste0(data_dir, "*.json*"))

  }else{

    if(is.null(file_paths)){
      stop(paste0("Please provide either 'data_dir' or 'file_paths'."))
    }

    if(!all(file.exists(file_paths))){
      warning(paste0("The following files do not exist and will be skipped:",
                     file_paths[!file.exists(file_paths)]
      ))
      json_files <- file_paths[file.exists(file_paths)]
    }else{

      json_files <- file_paths

    }
  }

  suppressWarnings(
    result_list <-  pbapply::pblapply(json_files, function(file) {
      parsed <- load_tweets_jsonl(file, ...)
      preprocessed <- preprocess_line_tweets(parsed)

      # Store if save_dir provided
      if(!is.null(save_dir)){

        if (!dir.exists(save_dir)) {
          stop(paste0("No such directory as ", save_dir))
        }

        if (!endsWith(save_dir, "/")) {
          save_dir <- paste0(save_dir, "/")
        }

        filepath <- paste0(save_dir, tools::file_path_sans_ext(basename(file)), "_preproc.rds")

        saveRDS(object = preprocessed, file = filepath)
      }


    }
    )
  )

  if(bind){
    result <- rbind_preprocessed_dts(result_list)
    return(result)
  }else{
    return(result_list)
  }

}
