test_that("s4h_standardize_dict validates input and converts", {
  expect_error(s4h_standardize_dict(list(a = 1)), "raw_dict must be an R data.frame")

  fake_mod <- list(
    s4h_standardize_dict = function(x) "py_res"
  )

  with_mocked_bindings(
    import = function(...) fake_mod,
    r_to_py = function(x) x,
    py_to_r = function(x) "r_res",
    .package = "reticulate",
    {
      df <- data.frame(question = "q", variable_name = "v", description = "d", value = "1")
      expect_equal(s4h_standardize_dict(df), "r_res")
    }
  )
})

test_that("s4h_translate_column validates input and converts", {
  expect_error(s4h_translate_column(list(a = 1), "col"), "data must be an R data.frame")

  fake_mod <- list(
    s4h_translate_column = function(df, column, language) "py_res"
  )

  with_mocked_bindings(
    import = function(...) fake_mod,
    r_to_py = function(x) x,
    py_to_r = function(x) "r_res",
    .package = "reticulate",
    {
      df <- data.frame(text = "hola")
      expect_equal(s4h_translate_column(df, "text", "en"), "r_res")
    }
  )
})

test_that("s4h_get_classifier returns python object", {
  fake_mod <- list(
    s4h_get_classifier = function(path) paste0("classifier:", path)
  )

  with_mocked_bindings(
    import = function(...) fake_mod,
    .package = "reticulate",
    {
      expect_equal(s4h_get_classifier("model"), "classifier:model")
    }
  )
})

test_that("s4h_classify_rows validates input and converts", {
  expect_error(s4h_classify_rows(list(a = 1), "a", "b", "c"), "data must be an R data.frame")

  fake_mod <- list(
    s4h_classify_rows = function(...) "py_res"
  )

  with_mocked_bindings(
    import = function(...) fake_mod,
    r_to_py = function(x) x,
    py_to_r = function(x) "r_res",
    .package = "reticulate",
    {
      df <- data.frame(a = "x", b = "y", c = "z")
      expect_equal(
        s4h_classify_rows(df, "a", "b", "c", new_column_name = "cat", MODEL_PATH = "m"),
        "r_res"
      )
    }
  )
})

test_that("s4h_parse_fwf_dict validates input and converts", {
  expect_error(s4h_parse_fwf_dict(list(a = 1)), "`dic` must be an R data.frame.")

  fake_mod <- list(
    s4h_parse_fwf_dict = function(dic) list(
      c("col1", "col2"),
      list(c(0, 1), c(1, 2))
    )
  )

  with_mocked_bindings(
    import = function(...) fake_mod,
    r_to_py = function(x) x,
    py_to_r = function(x) x,
    .package = "reticulate",
    {
      df <- data.frame(variable_name = "a", initial_position = 1, size = 1)
      res <- s4h_parse_fwf_dict(df)
      expect_equal(res$colnames, c("col1", "col2"))
      expect_length(res$colspecs, 2)
    }
  )
})
