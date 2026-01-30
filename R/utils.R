# R/utils.R
# Wrappers for socio4health.utils.harmonizer_utils

#' Standardize a Variable Dictionary
#'
#' Wrapper for `s4h_standardize_dict()` in Python.
#'
#' @param raw_dict R data.frame with columns
#'   \code{question}, \code{variable_name}, \code{description}, \code{value}, and optionally \code{subquestion}.
#'
#' @return Cleaned and grouped R data.frame.
#' @export
s4h_standardize_dict <- function(raw_dict) {
  if (!is.data.frame(raw_dict)) {
    stop("raw_dict must be an R data.frame.")
  }
  utils_mod <- reticulate::import("socio4health.utils.harmonizer_utils", delay_load = TRUE)
  df_py <- reticulate::r_to_py(raw_dict)
  res <- utils_mod$s4h_standardize_dict(df_py)
  reticulate::py_to_r(res)
}

#' Translate a Text Column with Google Translate
#'
#' @param data R data.frame.
#' @param column Name of the column to translate.
#' @param language Target language code (e.g., \code{"en"}).
#'
#' @return R data.frame with a new translated column.
#' @export
s4h_translate_column <- function(data, column, language = "en") {
  if (!is.data.frame(data)) {
    stop("data must be an R data.frame.")
  }
  utils_mod <- reticulate::import("socio4health.utils.harmonizer_utils", delay_load = TRUE)
  df_py <- reticulate::r_to_py(data)
  res <- utils_mod$s4h_translate_column(df_py, column = column, language = language)
  reticulate::py_to_r(res)
}

#' Get (and Cache) the BERT Classifier
#'
#' Wrapper for `s4h_get_classifier()` in Python.
#'
#' @param MODEL_PATH Path to the model (or HuggingFace model identifier).
#'
#' @return \code{transformers.Pipeline} object (Python).
#' @export
s4h_get_classifier <- function(MODEL_PATH = "./bert_finetuned_classifier") {
  utils_mod <- reticulate::import("socio4health.utils.harmonizer_utils", delay_load = TRUE)
  utils_mod$s4h_get_classifier(MODEL_PATH)
}

#' Classify Rows of a Data Frame with the BERT Model
#'
#' Wrapper for `s4h_classify_rows()` in Python.
#'
#' @param data R data.frame with text columns.
#' @param col1,col2,col3 Names of the text columns.
#' @param new_column_name Name of the new column with the category.
#' @param MODEL_PATH Path to the model (defaults to \code{"./bert_finetuned_classifier"}).
#'
#' @return R data.frame with a new prediction column.
#' @export
s4h_classify_rows <- function(data,
                              col1,
                              col2,
                              col3,
                              new_column_name = "category",
                              MODEL_PATH = "./bert_finetuned_classifier") {
  if (!is.data.frame(data)) {
    stop("data must be an R data.frame.")
  }
  utils_mod <- reticulate::import("socio4health.utils.harmonizer_utils", delay_load = TRUE)
  df_py <- reticulate::r_to_py(data)
  res <- utils_mod$s4h_classify_rows(
    data            = df_py,
    col1            = col1,
    col2            = col2,
    col3            = col3,
    new_column_name = new_column_name,
    MODEL_PATH      = MODEL_PATH
  )
  reticulate::py_to_r(res)
}


#' Parse FWF Dictionary with socio4health
#'
#' Wrapper for `s4h_parse_fwf_dict()` in
#' `socio4health.utils.extractor_utils`.
#'
#' @param dic R data.frame that contains at least the columns:
#'   - variable_name
#'   - initial_position
#'   - size  or  final_position
#'
#' @return List with:
#'   - colnames: vector of column names
#'   - colspecs: list of pairs (start, end) in base 0 (end exclusive)
#' @export
s4h_parse_fwf_dict <- function(dic) {
  if (!is.data.frame(dic)) {
    stop("`dic` must be an R data.frame.")
  }

  # Import the Python module where the function is located
  ext_utils <- reticulate::import(
    "socio4health.utils.extractor_utils",
    delay_load = TRUE
  )

  # Convert the R data.frame to pandas
  dic_py <- reticulate::r_to_py(dic)

  # Call the Python function
  res_py <- ext_utils$s4h_parse_fwf_dict(dic_py)

  # Convert the Python tuple to R object
  res_r <- reticulate::py_to_r(res_py)

  # res_r is a list of length 2: [[1]] colnames, [[2]] colspecs
  list(
    colnames = res_r[[1]],
    colspecs = res_r[[2]]
  )
}
