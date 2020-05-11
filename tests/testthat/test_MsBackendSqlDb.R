test_con <- dbConnect(SQLite(), "test.db")
test_con1 <- dbConnect(SQLite(), "test1.db")
fl <- msdata::proteomics(full.names = TRUE)[4]
test_be <- backendInitialize(MsBackendSqlDb(), test_con, fl)
test_tbl <- dbReadTable(test_con, "msdata")

test_that("initializeBackend,MsBackendSqlDb works", {
    expect_error(backendInitialize(MsBackendSqlDb()))
    expect_error(backendInitialize(MsBackendSqlDb(), test_con1),
                 "no such table: msdata")
    expect_error(backendInitialize(MsBackendSqlDb(), fl))
    expect_error(backendInitialize(MsBackendSqlDb(), 
                                   test_con,
                                   files = 4),
                 "invalid 'file' argument")
})


test_that("acquisitionNum, MsBackendSqlDb works", {
    expect_equal(acquisitionNum(test_be), test_tbl$acquisitionNum)
    expect_true(is(acquisitionNum(test_be), "integer"))
    expect_equal(acquisitionNum(test_be), 1:7534)
})

test_that("centroided, MsBackendMzR work", {
  expect_true(is(centroided(test_be), "logical"))
  expect_true(!all(centroided(test_be)))
})

test_that("length,MsBackendSqlDb works", {
  be <- MsBackendSqlDb()
  expect_equal(length(be), 0)
  be <- backendInitialize(MsBackendSqlDb(), dbcon = test_con, files = fl)
  expect_equal(length(be), nrow(test_tbl))
})
