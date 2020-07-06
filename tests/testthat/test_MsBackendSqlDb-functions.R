test_that(".write_data_to_db works", {
    expect_equal(.write_data_to_db(test_tbl[,1:3], test_con1, "test_tbl1"),
                 7534)
})

test_that(".get_db_data works", {
    expect_match(.get_db_data(test_be2, "Random"),
                 "Columns missing from database")
    expect_equal(.get_db_data(test_be2, "totIonCurrent"), 
                 test_tbl$totIonCurrent) 
    expect_equal(.get_db_data(test_be2, "acquisitionNum"), 
                 test_tbl$acquisitionNum)
})

test_that(".valid_db_table_exists works", {
    expect_null(.valid_db_table_exists(con, "msdata"))
    expect_match(.valid_db_table_exists(con, "foobar"), "not found")
})

test_that(".valid_db_table_columns works", {
    expect_null(.valid_db_table_columns(con, "msdata", pkey = "pkey"))
    expect_match(.valid_db_table_columns(con, "msdata", pkey = "_pkey"),
                 "required .* not found")
    expect_match(.valid_db_table_columns(con, "msdata",
                                         columns = "foo", pkey = "pkey"),
                 "required .* foo not found")
    
    msdf2 <- msdf
    msdf2$msLevel <- as.character(msdf2$msLevel)
    DBI::dbWriteTable(con, "msdata2test", msdf2)
    expect_match(.valid_db_table_columns(con, "msdata2test", pkey = "pkey"),
                 "wrong data type")
})


##
##    Tests for subsetting and filtering
##

test_that(".subset_backend_SqlDb works", {
    expect_identical(.subset_backend_SqlDb(test_be), test_be)
    res <- .subset_backend_SqlDb(test_be, 13)
    expect_true(is(res, "MsBackendSqlDb"))
    expect_equal(res@rows, test_be@rows[13])
})

test_that(".sel_file works", {
    df <- data.frame(msLevel = 1L,
                     dataOrigin = c("a", "a", "a", "a", "b", "c"),
                     dataStorage = rep("<db>", 6),
                     pkey = 1L:6L)
    DBI::dbWriteTable(test_con, "msdf", df)
    ## The following SQL statement are used to rename `pkey` to `_pkey`
    dbExecute(test_con,
              "ALTER TABLE msdf RENAME TO temp_msdf;")
    type <- dbDataType(test_con, df)[-length(df)]
    str <- paste(names(type), type, sep = " ")
    str <- paste(str, collapse=', ')
    sql <- paste("CREATE TABLE msdf (",
                  str,
                  ", _pkey INT PRIMARY KEY);", sep = "")
    dbExecute(test_con, sql)
    sql_1 <- paste("INSERT INTO msdf(", 
                    paste(names(type), collapse = ", "),
                    ", _pkey) ",
                    "SELECT * FROM temp_msdf;")
    dbExecute(test_con, sql_1)
    dbExecute(test_con, "DROP TABLE temp_msdf;")
    
    ## We don't have to compare the results of dataStorage
    ## It will be a permanent address "<db>"
    ## We may change this behavior after using parallel processing
    sdb <- test_be
    sdb@dbtable <- "msdf"
    sdb@rows <- 1:6
    sdb@columns <- c("msLevel", "dataOrigin", "dataStorage")
    res <- .sel_file_sql(sdb)
    expect_identical(res, rep(TRUE, length(sdb)))
    res <- .sel_file_sql(sdb, dataStorage = c("c", "a"))
    expect_identical(res, rep(FALSE, length(sdb)))
    
    res <- .sel_file_sql(sdb, dataOrigin = c("c", "a"))
    expect_identical(res, c(TRUE, TRUE, TRUE, TRUE, FALSE, TRUE))
    res <- .sel_file_sql(sdb, dataOrigin = "z")
    expect_identical(res, rep(FALSE, length(sdb)))
    res <- .sel_file_sql(sdb, dataOrigin = 3)
    expect_identical(res, c(FALSE, FALSE, FALSE, FALSE, FALSE, TRUE))
    res <- .sel_file_sql(sdb, dataOrigin = NA_character_)
    expect_identical(res, rep(FALSE, length(sdb)))
    expect_error(.sel_file_sql(sdb, dataOrigin = TRUE), 
                 "integer with the index")
    res <- .sel_file_sql(sdb, dataOrigin = c("b", "z"))
    expect_identical(res, c(FALSE, FALSE, FALSE, FALSE, TRUE, FALSE))
})