test_that("s4h_get_default_data_dir calls python helper", {
  fake_mod <- list(
    s4h_get_default_data_dir = function() "/tmp/data"
  )

  with_mocked_bindings(
    import = function(...) fake_mod,
    .package = "reticulate",
    {
      expect_equal(s4h_get_default_data_dir(), "/tmp/data")
    }
  )
})

test_that("s4h_extractor passes arguments through", {
  fake_module <- list(
    Extractor = function(...) list(args = list(...))
  )

  with_mocked_bindings(
    .s4h_get_module = function() fake_module,
    {
      res <- s4h_extractor(
        input_path = "x",
        depth = 2,
        down_ext = c(".csv"),
        encoding = "utf8"
      )
      expect_equal(res$args$input_path, "x")
      expect_equal(res$args$depth, 2)
      expect_equal(res$args$down_ext, c(".csv"))
      expect_equal(res$args$encoding, "utf8")
    }
  )
})

test_that("s4h_extract returns dask, pandas, or data.frame", {
  ddf1 <- list(compute = function() "pd1")
  ddf2 <- list(compute = function() "pd2")
  extractor <- list(
    s4h_extract = function() list(ddf1, ddf2)
  )

  with_mocked_bindings(
    py_to_r = function(x) x,
    .package = "reticulate",
    {
      expect_equal(s4h_extract(extractor, "dask"), list(ddf1, ddf2))
      expect_equal(s4h_extract(extractor, "pandas"), list("pd1", "pd2"))
      expect_equal(s4h_extract(extractor, "data.frame"), list("pd1", "pd2"))
    }
  )
})

test_that("s4h_delete_download_folder forwards optional path", {
  extractor <- list(
    s4h_delete_download_folder = function(folder_path = NULL) {
      if (is.null(folder_path)) TRUE else folder_path
    }
  )

  expect_true(s4h_delete_download_folder(extractor))
  expect_equal(s4h_delete_download_folder(extractor, "c:/tmp"), "c:/tmp")
})

