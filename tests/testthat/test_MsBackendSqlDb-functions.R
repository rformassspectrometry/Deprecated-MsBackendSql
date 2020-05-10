test_con <- dbConnect(SQLite(), tempfile())
fl <- msdata::proteomics(full.names = TRUE)[4]
test_be <- backendInitialize(MsBackendSqlDb(), test_con, fl)
test_tbl <- dbReadTable(test_con, "msdata")

test_that(".valid_db_table_columns", {
  expect_null(.valid_db_table_columns(
    test_con, 'nottable'))
})

