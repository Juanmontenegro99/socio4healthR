test_that("s4h_harmonizer passes arguments and coerces min_common_columns", {
  fake_module <- list(
    Harmonizer = function(...) list(args = list(...))
  )

  with_mocked_bindings(
    .s4h_get_module = function() fake_module,
    {
      res <- s4h_harmonizer(min_common_columns = 2.7, similarity_threshold = 0.5)
      expect_equal(res$args$min_common_columns, 2L)
      expect_equal(res$args$similarity_threshold, 0.5)
    }
  )
})

test_that("s4h_vertical_merge converts output by return_as", {
  ddf1 <- list(compute = function() "pd1")
  ddf2 <- list(compute = function() "pd2")
  harmonizer <- list(
    s4h_vertical_merge = function(ddfs) list(ddf1, ddf2)
  )

  with_mocked_bindings(
    py_to_r = function(x) x,
    .package = "reticulate",
    {
      expect_equal(s4h_vertical_merge(harmonizer, list("a"), "dask"), list(ddf1, ddf2))
      expect_equal(s4h_vertical_merge(harmonizer, list("a"), "pandas"), list("pd1", "pd2"))
      expect_equal(s4h_vertical_merge(harmonizer, list("a"), "data.frame"), list("pd1", "pd2"))
    }
  )
})

test_that("s4h_drop_nan_columns handles list and single return values", {
  ddf1 <- list(compute = function() "pd1")
  ddf2 <- list(compute = function() "pd2")

  harmonizer_list <- list(
    drop_nan_columns = function(x) structure(list(ddf1, ddf2), class = "python.builtin.list")
  )
  harmonizer_one <- list(
    drop_nan_columns = function(x) ddf1
  )

  with_mocked_bindings(
    py_to_r = function(x) x,
    .package = "reticulate",
    {
      res_list <- s4h_drop_nan_columns(harmonizer_list, list("a"), "data.frame")
      expect_equal(res_list, list("pd1", "pd2"))

      res_one <- s4h_drop_nan_columns(harmonizer_one, list("a"), "pandas")
      expect_equal(res_one, "pd1")
    }
  )
})

test_that("s4h_get_available_columns calls static method", {
  fake_module <- list(
    Harmonizer = list(
      s4h_get_available_columns = function(x) c("col1", "col2")
    )
  )

  with_mocked_bindings(
    .s4h_get_module = function() fake_module,
    {
      expect_equal(s4h_get_available_columns("x"), c("col1", "col2"))
    }
  )
})

test_that("s4h_harmonize_dataframes converts and formats output", {
  ddf1 <- list(compute = function() data.frame(a = 1))
  ddf2 <- list(compute = function() data.frame(a = 2))

  res <- list(
    keys = function() c("CO", "BR"),
    CO = list(ddf1),
    BR = list(ddf2)
  )

  harmonizer <- list(
    s4h_harmonize_dataframes = function(country_dfs) res
  )

  with_mocked_bindings(
    r_to_py = function(x) x,
    py_to_r = function(x) x,
    .package = "reticulate",
    {
      out <- s4h_harmonize_dataframes(harmonizer, list(CO = list("x")), "data.frame")
      expect_equal(names(out), c("CO", "BR"))
      expect_true(is.data.frame(out[[1]][[1]]))
    }
  )
})

test_that("s4h_compare_with_dict returns R or Python object", {
  harmonizer <- list(
    s4h_compare_with_dict = function(ddfs) "py_df"
  )

  with_mocked_bindings(
    py_to_r = function(x) "r_df",
    .package = "reticulate",
    {
      expect_equal(s4h_compare_with_dict(harmonizer, list("a"), TRUE), "r_df")
      expect_equal(s4h_compare_with_dict(harmonizer, list("a"), FALSE), "py_df")
    }
  )
})

test_that(".s4h_convert_ddf_list handles dask, pandas, and data.frame", {
  ddf <- structure(
    list(compute = function() "pd"),
    has_compute = TRUE
  )
  df_pandas <- "pd2"

  with_mocked_bindings(
    py_has_attr = function(x, name) isTRUE(attr(x, "has_compute")) && name == "compute",
    py_to_r = function(x) x,
    .package = "reticulate",
    {
      expect_equal(.s4h_convert_ddf_list(list(ddf), "dask"), list(ddf))
      expect_equal(.s4h_convert_ddf_list(list(ddf), "pandas"), list("pd"))
      expect_equal(.s4h_convert_ddf_list(list(df_pandas), "data.frame"), list("pd2"))
    }
  )
})

test_that("s4h_data_selector delegates and converts", {
  ddf <- structure(
    list(compute = function() "pd"),
    has_compute = TRUE
  )
  harmonizer <- list(
    s4h_data_selector = function(ddfs) list(ddf)
  )

  with_mocked_bindings(
    py_has_attr = function(x, name) isTRUE(attr(x, "has_compute")) && name == "compute",
    py_to_r = function(x) x,
    .package = "reticulate",
    {
      out <- s4h_data_selector(harmonizer, list("a"), "data.frame")
      expect_equal(out, list("pd"))
    }
  )
})

test_that("s4h_join_data returns R or Python data", {
  harmonizer <- list(
    s4h_join_data = function(ddfs) "py_df"
  )

  with_mocked_bindings(
    py_to_r = function(x) "r_df",
    .package = "reticulate",
    {
      expect_equal(s4h_join_data(harmonizer, list("a"), TRUE), "r_df")
      expect_equal(s4h_join_data(harmonizer, list("a"), FALSE), "py_df")
    }
  )
})

test_that("s4h_harmonizer_call invokes method or errors", {
  harmonizer <- list(
    foo = function(x) x + 1
  )

  expect_equal(s4h_harmonizer_call(harmonizer, "foo", 1), 2)
  expect_error(
    s4h_harmonizer_call(harmonizer, "bar"),
    "does not exist"
  )
})
