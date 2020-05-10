test_con <- dbConnect(SQLite(), tempfile())
fl <- msdata::proteomics(full.names = TRUE)[4]
test_be <- backendInitialize(MsBackendSqlDb(), test_con, fl)
test_tbl <- dbReadTable(test_con, "msdata")

test_that("initializeBackend,MsBackendSqlDb works", {
  be <- backendInitialize(MsBackendSqlDb(), test_con, fl)
  expect_true(validObject(be))
})


test_that("length,MsBackendSqlDb works", {
  be <- MsBackendSqlDb()
  expect_equal(length(be), 0)
  be <- backendInitialize(MsBackendSqlDb(), dbcon = test_con, files = fl)
  expect_equal(length(be), nrow(test_tbl))
})

test_that("acquisitionNum, MsBackendSqlDb works", {
  be <- backendInitialize(MsBackendSqlDb(), dbcon = test_con, files = fl)
  expect_equal(acquisitionNum(be), test_tbl$acquisitionNum)
})