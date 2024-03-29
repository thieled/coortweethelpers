% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/parse_by_line.R
\name{load_users_jsonl}
\alias{load_users_jsonl}
\title{Load Users from JSON or JSONL File}
\usage{
load_users_jsonl(
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
)
}
\arguments{
\item{file_path}{A character string specifying the path to the .json or .jsonl file.}

\item{num_threads}{An integer specifying the number of threads to be used for parsing. Default is NULL.}

\item{n_max}{An integer specifying the maximum number of lines to read from the file. Default is Inf.}

\item{skip_empty_rows}{A logical value indicating whether empty rows should be skipped. Default is TRUE.}

\item{query}{A character string specifying the JSON path to extract user data from. Default is "/includes/users".}

\item{query_error_ok}{A logical value indicating whether to continue parsing if a query error occurs. Default is TRUE.}

\item{on_query_error}{A function to handle query errors if they occur. Default is NULL.}

\item{parse_error_ok}{A logical value indicating whether to continue parsing if a parse error occurs. Default is TRUE.}

\item{on_parse_error}{A function to handle parse errors if they occur. Default is NULL.}

\item{empty_array}{A value to use for empty arrays encountered during parsing. Default is NULL.}

\item{empty_object}{A value to use for empty objects encountered during parsing. Default is NULL.}

\item{max_simplify_lvl}{An integer specifying the maximum level of simplification. Default is 2L (vector).
Other options from RcppSimdJson::fparse are data.frame (0L), matrix (1L), vector (2L) or list (3L).}
}
\value{
A data.table object containing the parsed and reshaped user data.
}
\description{
This function parses and reshapes .json and .jsonl data containing user information into a format suitable for further analysis.
}
\details{
This function reads a .json or .jsonl file line by line to avoid potential errors with RcppSimdJson. It then processes the user data into a format suitable for further analysis.
}
\examples{
\dontrun{
  users <- load_users_json_line("tweets.json")
}

}
\references{
For more information on the CooRTweet package, see: https://github.com/nicolarighetti/CoRTweet
}
