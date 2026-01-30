# R/extractor.R
# R Wrappers for socio4health.Extractor

#' Get socio4health Default Data Directory
#'
#' Wrapper for `s4h_get_default_data_dir()` Python function.
#'
#' @return Character string with the path to the directory where socio4health stores downloads.
#'
#' @examples
#' \dontrun{
#'   data_dir <- s4h_get_default_data_dir()
#'   print(data_dir)
#' }
#'
#' @export
s4h_get_default_data_dir <- function() {
  extractor_mod <- reticulate::import("socio4health.extractor", delay_load = TRUE)
  path <- extractor_mod$s4h_get_default_data_dir()
  as.character(path)
}

#' Create a socio4health `Extractor` Object
#'
#' Direct wrapper for the `Extractor.__init__` constructor in Python.
#' See \code{?socio4health::Extractor} for a detailed description of each argument.
#'
#' @param input_path Path (URL or local folder). Required.
#' @param depth Scraping depth (when \code{input_path} is a URL).
#' @param down_ext Vector of file extensions to download (e.g., \code{c(".zip", ".csv")}).
#' @param output_path Output directory (defaults to socio4health data directory).
#' @param key_words Vector of keywords to filter links.
#' @param encoding Character encoding for reading files. Defaults to \code{"latin1"}.
#' @param is_fwf \code{TRUE} if files are fixed-width format.
#' @param colnames Column names for FWF (required if \code{is_fwf = TRUE}).
#' @param colspecs Column specifications for FWF.
#' @param sep Separator for CSV/TXT files. If \code{NULL}, uses internal logic.
#' @param ddtype Data type for Dask (string or dict). Defaults to \code{"object"}.
#' @param dtype Data type(s) for pandas.
#' @param engine Engine for reading Excel files.
#' @param sheet_name Sheet name/index/vector of sheet names.
#' @param geodriver Driver for reading geospatial files.
#'
#' @return Python `Extractor` object.
#'
#' @examples
#' \dontrun{
#'   data_dir <- s4h_extractor(
#'     input_path = "https://example.com/data",
#'     down_ext = c(".csv", ".xlsx"),
#'     key_words = c("health", "socioeconomic")
#'   )
#' }
#'
#' @export
s4h_extractor <- function(input_path,
                          depth = NULL,
                          down_ext = NULL,
                          output_path = NULL,
                          key_words = NULL,
                          encoding = "latin1",
                          is_fwf = FALSE,
                          colnames = NULL,
                          colspecs = NULL,
                          sep = NULL,
                          ddtype = "object",
                          dtype = NULL,
                          engine = NULL,
                          sheet_name = NULL,
                          geodriver = NULL) {
  mod <- .s4h_get_module()
  extractor_cls <- mod$Extractor

  extractor_cls(
    input_path  = input_path,
    depth       = depth,
    down_ext    = down_ext,
    output_path = output_path,
    key_words   = key_words,
    encoding    = encoding,
    is_fwf      = is_fwf,
    colnames    = colnames,
    colspecs    = colspecs,
    sep         = sep,
    ddtype      = ddtype,
    dtype       = dtype,
    engine      = engine,
    sheet_name  = sheet_name,
    geodriver   = geodriver
  )
}

#' Execute `s4h_extract()` from an `Extractor` Object
#'
#' Wrapper for `Extractor.s4h_extract()` in Python.
#' Depending on \code{return_as}, converts the list of Dask DataFrames to
#' pandas or R data.frames.
#'
#' @param extractor Python `Extractor` object (returned by \code{s4h_extractor()}).
#' @param return_as \code{"dask"}, \code{"pandas"}, or \code{"data.frame"}.
#'
#' @return
#' * \code{"dask"}: list of Dask objects (Python).
#' * \code{"pandas"}: list of pandas DataFrames (Python).
#' * \code{"data.frame"}: list of R data.frames.
#' @export
s4h_extract <- function(extractor,
                            return_as = c("dask", "pandas", "data.frame")) {
  return_as <- match.arg(return_as)

  res <- extractor$s4h_extract()  # list of Dask DataFrames

  if (return_as == "dask") {
    return(res)
  }

  out <- lapply(res, function(ddf) {
    if (return_as == "pandas") {
      ddf$compute()
    } else {
      reticulate::py_to_r(ddf$compute())
    }
  })

  out
}

#' Delete the Download Folder from an `Extractor` Object
#'
#' Wrapper for `Extractor.s4h_delete_download_folder()`.
#'
#' @param extractor Python `Extractor` object.
#' @param folder_path Specific path to delete (optional).
#'
#' @return \code{TRUE}/\code{FALSE} based on the result from Python.
#' @export
s4h_delete_download_folder <- function(extractor, folder_path = NULL) {
  if (is.null(folder_path)) {
    extractor$s4h_delete_download_folder()
  } else {
    extractor$s4h_delete_download_folder(folder_path = folder_path)
  }
}
