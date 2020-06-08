msdf <- data.frame(
    pkey = 1L:3L,
    rtime = c(1.2, 3.4, 5.6),
    msLevel = c(1L, 2L, 2L),
    dataStorage = "<db>",
    dataOrigin = "file.mzML",
    stringsAsFactors = FALSE
)
msdf$mz <- lapply(1:3, serialize, NULL)
msdf$intensity <- lapply(4:6, serialize, NULL)

con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
on.exit(DBI::dbDisconnect(con))

DBI::dbWriteTable(con, "msdata", msdf)

test_that(".valid_db_table_exists", {
    expect_null(.valid_db_table_exists(con, "msdata"))
    expect_match(.valid_db_table_exists(con, "foobar"), "not found")
})

test_that(".valid_db_table_columns", {
    expect_null(.valid_db_table_columns(con, "msdata", pkey = "pkey"))
    expect_match(.valid_db_table_columns(con, "msdata", pkey = "_pkey"),
                 "required .* not found")
    expect_match(.valid_db_table_columns(con, "msdata",
                                         columns = "foo", pkey = "pkey"),
                 "required .* foo not found")

    msdf2 <- msdf
    msdf2$msLevel <- as.character(msdf2$msLevel)
    DBI::dbWriteTable(con, "msdata2", msdf2)
    expect_match(.valid_db_table_columns(con, "msdata2", pkey = "pkey"),
                 "wrong data type")
})
