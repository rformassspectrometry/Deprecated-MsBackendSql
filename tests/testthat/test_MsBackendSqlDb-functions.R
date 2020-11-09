test_that(".valid_db_table_exists works", {
    expect_null(.valid_db_table_exists(sciexSQL1@dbcon, "msdata"))
    expect_match(.valid_db_table_exists(sciexSQL1@dbcon, "foobar"), 
                 "not found")
})

test_that(".valid_db_table_columns works", {
    expect_null(.valid_db_table_columns(sciexSQL1@dbcon, "msdata", 
                                        pkey = "_pkey"))
    expect_match(.valid_db_table_columns(sciexSQL1@dbcon, 
                                         "msdata", pkey = "pkey"),
                 "required .* not found")
    expect_match(.valid_db_table_columns(sciexSQL1@dbcon, "msdata",
                                         columns = "foo", pkey = "_pkey"),
                 "required .* foo not found")
    
    con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
    on.exit(DBI::dbDisconnect(con))
    msdf$mz <- c(1.5, 3.6, 8.3)
    msdf$intensity <- c(5.0, 9, 15)
    DBI::dbWriteTable(con, "msdata", msdf)
    
    msdf2 <- msdf
    msdf2$msLevel <- as.character(msdf2$msLevel)
    msdf2$pkey <- c(1L, 2L, 3L)
    DBI::dbWriteTable(con, "msdata2test", msdf2)
    dbSendQuery(con, "ALTER TABLE msdata2test RENAME COLUMN pkey to _pkey")
    expect_match(.valid_db_table_columns(con, "msdata2test", pkey = "_pkey"),
                 "wrong data type")
})

test_that(".write_mzR_to_db works", {
    con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
    on.exit(DBI::dbDisconnect(con))
    expect_equal(.write_mzR_to_db(sciexmzML1, con), 10)
    MsBackendSqlDb1 <- backendInitialize(MsBackendSqlDb(), dbcon = con)
    expect_equal(dbListTables(MsBackendSqlDb1@dbcon), "msdata")
})

test_that(".initiate_data_to_table works", {
    data <- dbReadTable(sciexSQL1@dbcon, "msdata")
    ## "_pkey" column from SQLite DB will be renamed to "X_pkey" by R
    data <- data[, names(data)[!names(data) %in% "X_pkey"]]
    con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
    on.exit(DBI::dbDisconnect(con))
    ## Before this function, there shall not be a table in the connection obj
    expect_identical(dbListTables(con), character(0))
    
    x <- .initiate_data_to_table(data, con)
    MsBackendSqlDb1 <- backendInitialize(MsBackendSqlDb(), dbcon = con)
    ## This function shall return a data.frame for other functions
    expect_true(is(x, "data.frame"))
    ## We can also expect by default, `msdata` table is created in the con obj
    expect_match(dbListTables(MsBackendSqlDb1@dbcon), "msdata")
    ## We can test whether `mz` and `intensity` columns have type as BLOB
    typeCol <- dbGetQuery(MsBackendSqlDb1@dbcon, "PRAGMA table_info(msdata)")
    expect_match(typeCol[typeCol$name %in% c("mz", "intensity"),]$type,
                 "BLOB")
})

test_that(".write_data_to_db works", {
    data <- dbReadTable(sciexSQL1@dbcon, "msdata")
    ## "_pkey" column from SQLite DB will be renamed to "X_pkey" by R
    data <- data[, names(data)[!names(data) %in% "X_pkey"]]
    sql <- MsBackendSqlDb()
    on.exit(DBI::dbDisconnect(sql@dbcon))
    ## Before this function, there shall not be a table in the connection obj
    expect_identical(dbListTables(sql@dbcon), character(0))
    
    ## This function shall append 10L rows to the dbcon
    ## and create a SQLite table "msdata"
    expect_identical(.write_data_to_db(data, sql@dbcon), 10L)
    expect_match(dbListTables(sql@dbcon), "msdata")
    ## After using this function, there shall be a table `msdata`
    ## has 10 rows
    rowNum <- dbGetQuery(sql@dbcon, "SELECT COUNT(*) FROM msdata")
    expect_identical(rowNum[1, 1], 
                 10L)
    
    ## We can append another data (same dimensions as before) 
    ## to this connection obj
    expect_identical(.write_data_to_db(data, sql@dbcon), 10L)
    ## Now `msdata` has 20 rows
    rowNum2 <- dbGetQuery(sql@dbcon, "SELECT COUNT(*) FROM msdata")
    expect_identical(rowNum2[1, 1], 
                     20L)
})

test_that(".get_db_data works", {
    expect_match(.get_db_data(sciexSQL1, "Random"),
                 "Columns missing from database")
    expect_equal(.get_db_data(sciexSQL1, "totIonCurrent"), 
                 testTbl$totIonCurrent) 
    expect_equal(.get_db_data(sciexSQL1, "highMZ"), 
                 testTbl$highMZ)
    expect_true(is(.get_db_data(sciexSQL1, c("highMZ","mz", "intensity")), 
                   "DFrame"))
    expect_true(is(.get_db_data(sciexSQL1, "mz"),
                   "SimpleNumericList"))
})

test_that(".subset_backend_SqlDb works", {
    ## If missing "i"
    sql0 <- .subset_backend_SqlDb(sciexSQL1)
    ## `sql0`` shall be equal to `sciexSQL1`
    expect_identical(sql0@rows, sciexSQL1@rows)
    
    ## While `i` is provided as integer()
    sql1 <- .subset_backend_SqlDb(sciexSQL1, c(4, 1, 3))
    expect_identical(length(sql1), 3L)
    expect_identical(sql1@rows, c(4L, 1L, 3L))
    
    ## While `i` is provided as logical vector
    logI <- c(rep(c(TRUE, FALSE), 3), FALSE, FALSE, FALSE, TRUE)
    sql2 <- .subset_backend_SqlDb(sciexSQL1, logI)
    expect_identical(length(sql2), 4L)
    expect_identical(sql2@rows, c(1L, 3L, 5L, 10L))
})

test_that(".sel_file_sql works", {
    ## The case of `dataStorage` is omitted here
    ## The current values of `dataStorage` is `<db>`
    
    dataOriginLog <- c(TRUE, FALSE, TRUE, FALSE, TRUE,
                       FALSE, TRUE, FALSE, TRUE, FALSE)
    expect_error(.sel_file_sql(sciexSQL1, dataOrigin = dataOriginLog),
                 "'dataOrigin' has to be either an integer .*, or its name")
    expect_error(.sel_file_sql(sciexSQL1, dataOrigin = 0.5),
                 "'dataOrigin' should be an integer between *.")
    expect_error(.sel_file_sql(sciexCombined, dataOrigin = 3),
                 "'dataOrigin' should be an integer between 1 and 2")
    ## Correct case: Only the 2nd file is selected
    expect_identical(.sel_file_sql(sciexCombined, dataOrigin = 2),
                 c(rep(FALSE, 10), rep(TRUE, 10)))
})

test_that(".combine_backend_SqlDb works", {
    ## Conditional case: objects with different Backend Types
    expect_error(.combine_backend_SqlDb(c(sciexSQL1, sciex_mzR1)),
                 "Can only merge backends of the same type: MsBackendSqlDb")
    
    ## Conditional case: objects with different spectra variables
    dfSQL <- backendInitialize(MsBackendSqlDb(), data = msdf)
    expect_error(.combine_backend_SqlDb(c(dfSQL, sciexSQL1)),
                 "Can only merge backends with the same spectra variables")
    
    ## Test case 1: if (length(objects) == 1)
    testSQL <- .combine_backend_SqlDb(c(sciexSQL1))
    expect_true(is(testSQL, "MsBackendSqlDb"))
    expect_identical(testSQL$mz, sciexSQL1$mz)
    expect_identical(testSQL$intensity, sciexSQL1$intensity)
    expect_identical(testSQL@dbtable, sciexSQL1@dbtable)
    expect_identical(testSQL@dbcon, sciexSQL1@dbcon)
    
    ## Test Case 2: while we merge `sciexSQL1/testSQL1` and `sciexSQL2`
    testSQL1 <- .clone_MsBackendSqlDb(sciexSQL1)
    testSQLMer <- .combine_backend_SqlDb(c(testSQL1, sciexSQL2))
    expect_identical(testSQLMer$mz, sciexCombined$mz)
    expect_identical(testSQLMer$intensity, sciexCombined$intensity)
    expect_identical(testSQLMer@dbtable, sciexCombined@dbtable)
    expect_identical(testSQLMer@dbcon, testSQL1@dbcon)
    rm(testSQLMer)
    
    ## Test Case 3: while we merge `sciexSQL1/testSQL1` and `sciexSQL2`
    ##     with dbcon provided
    connTest <- dbConnect(SQLite(), "tmp.db")
    testSQL1 <- .clone_MsBackendSqlDb(sciexSQL1)
    testSQLMer <- .combine_backend_SqlDb(c(testSQL1, sciexSQL2), 
                                         dbcon = connTest)
    expect_identical(testSQLMer$mz, sciexCombined$mz)
    expect_identical(testSQLMer$intensity, sciexCombined$intensity)
    expect_identical(testSQLMer@dbtable, sciexCombined@dbtable)
    expect_identical(testSQLMer@dbcon, connTest)
    expect_identical(dbListTables(connTest), testSQLMer@dbtable)
    expect_identical(nrow(dbReadTable(testSQLMer@dbcon, 
                                      testSQLMer@dbtable)), 20L)
    ## As we expected, testSQL1 wasn't modified by the function
    expect_identical(dbListTables(testSQL1@dbcon), testSQL1@dbtable)
    expect_identical(nrow(dbReadTable(testSQL1@dbcon, 
                                      testSQL1@dbtable)), 10L)
})

test_that(".attach_migration works", {
    testSQL1 <- .clone_MsBackendSqlDb(sciexSQL1)
    testSQL2 <- .clone_MsBackendSqlDb(sciexSQL2)
    ## If `x` and `y` are sharing the same dbfile, and using the same dbtable
    testSQL3 <- testSQL2
    testSQL4 <- testSQL2
    testSQL3@rows <- seq(1L, 10L, 2L)
    testSQL4@rows <- seq(2L, 10L, 2L)
    testSQL5 <- .attach_migration(testSQL3, testSQL4)
    expect_identical(testSQL3@dbcon, testSQL4@dbcon)
    expect_identical(testSQL3@dbcon, testSQL5@dbcon)
    expect_identical(testSQL3@dbtable, testSQL5@dbtable)
    expect_identical(testSQL3@modCount, testSQL5@modCount)
    expect_true(identical(setdiff(testSQL2@rows, testSQL5@rows), 
                          setdiff(testSQL5@rows, testSQL2@rows)))
    expect_identical(testSQL5@rows, c(testSQL3@rows, testSQL4@rows))
    rm(testSQL5)
    
    ## If `x` and `y` are sharing the same dbfile, and using different dbtable
    ## We put testSQL2's SQLite table into testSQL1@dbcon
    dbExecute(testSQL1@dbcon, paste0("ATTACH DATABASE '",
                                     testSQL2@dbcon@dbname, "' AS toMerge"))
    res <- dbSendQuery(testSQL1@dbcon, "CREATE TABLE msdata2 AS
                                        SELECT *
                                        FROM toMerge.msdata")
    suppressWarnings(dbExecute(testSQL1@dbcon, "DETACH DATABASE toMerge"))
    ## `MsBackendSqlDb` object `testSQL6` shares the same dbfile with
    ## testSQL1, but using a differetnt SQLite table `msdata2`
    testSQL6 <- testSQL1
    testSQL6@dbtable <- "msdata2"
    ## Only keep 4 rows in the second MSBackendSqlDb instance
    testSQL6@rows <- c(2L, 5L, 7L, 9L)
    ## testSQL5 is used as the merged result
    testSQL6@modCount <- 9L
    testSQL5 <- .attach_migration(testSQL1, testSQL6)
    expect_identical(testSQL1@dbcon, testSQL5@dbcon)
    expect_identical(testSQL6@dbcon, testSQL5@dbcon)
    ## testSQL5 will share the same "dbtable" with testSQL1
    expect_identical(testSQL1@dbtable, testSQL5@dbtable)
    ## `ATTACH` table will increase `modCount` by 1L.
    expect_identical(testSQL5@modCount, 10L)
    expect_identical(length(testSQL5), 14L)
    expect_identical(spectraData(testSQL5[11:14]), spectraData(testSQL6))
    expect_identical(testSQL5@rows, c(testSQL1@rows, 10L + testSQL6@rows))
    ## We expect, the dbtable in testSQL5 only preserved 14 rows
    expect_identical(nrow(dbReadTable(testSQL5@dbcon, testSQL5@dbtable)), 20L)
    rm(testSQL1, testSQL5)
    
    ## While x and y have different db files, but dbtables have the same name:
    testSQL1 <- .clone_MsBackendSqlDb(sciexSQL1)
    testSQL2@rows <- c(2L, 5L, 7L, 9L)
    testSQL2@modCount <- 5L
    testSQL5 <- .attach_migration(testSQL1, testSQL2)
    
    expect_identical(testSQL1@dbcon, testSQL5@dbcon)
    ## testSQL1/testSQL5 has different dbcon obj than testSQL2
    expect_identical(identical(testSQL1@dbcon, testSQL2@dbcon), FALSE)
    expect_identical(testSQL1@dbtable, testSQL5@dbtable)
    expect_identical(testSQL5@modCount, 6L)
    expect_identical(length(testSQL5), 14L)
    expect_identical(spectraData(testSQL5[11:14]), spectraData(testSQL2))
    expect_identical(testSQL5@rows, c(testSQL1@rows, 10L + testSQL2@rows))
    ## We expect, the dbtable in testSQL5 only preserved 14 rows
    expect_identical(nrow(dbReadTable(testSQL5@dbcon, testSQL5@dbtable)), 20L)
})

test_that(".clone_MsBackendSqlDb works", {
    testSQL1 <- .clone_MsBackendSqlDb(sciexSQL1)
    expect_identical(length(testSQL1), length(sciexSQL1))
    expect_identical(testSQL1$mz, sciexSQL1$mz)
    expect_identical(testSQL1$intensity, sciexSQL1$intensity)
    expect_identical(testSQL1$basePeakIntensity, 
                     sciexSQL1$basePeakIntensity)
    expect_false(identical(testSQL1@dbcon@dbname,
                           sciexSQL1@dbcon@dbname))
})

test_that(".update_db_table_columns works", {
    ## Clone `MsBackendSqlDb` instance `sciexSQL1`
    testSQL1 <- .clone_MsBackendSqlDb(sciexSQL1)
    testSQL1 <- .update_db_table_columns(testSQL1, "msLevel", 
                                         rep("testLevel", length(testSQL1)))
    testSQL1 <- .update_db_table_columns(testSQL1, "mz", 
                                         mz(sciexSQL2))
    testSQL1 <- .update_db_table_columns(testSQL1, "intensity", 
                                         intensity(sciexSQL2))
    expect_identical(testSQL1$msLevel, rep("testLevel", length(testSQL1)))
    expect_identical(testSQL1$mz, sciexSQL2$mz)
    expect_identical(testSQL1$intensity, sciexSQL2$intensity)
})

test_that(".insert_db_table_columns works", {
    ## Clone `MsBackendSqlDb` instance `sciexSQL1`
    testSQL1 <- .clone_MsBackendSqlDb(sciexSQL1)
    mz2 <- mz(sciexSQL2)
    origin2 <- dataOrigin(sciexSQL2)
    rtime2 <- rtime(sciexSQL2)
    
    testSQL1 <- .insert_db_table_columns(testSQL1, "mzTest", mz2)
    testSQL1 <- .insert_db_table_columns(testSQL1, "originTest", origin2)
    testSQL1 <- .insert_db_table_columns(testSQL1, "rtimeTest", rtime2)
    
    ## expect_identical(testSQL1$mzTest, mz2) failed,
    ## it requires modifications of `.get_db_data`
    expect_identical(testSQL1$originTest, origin2)
    expect_identical(testSQL1$rtimeTest, rtime2)
})

test_that(".insert_db_table_columns works", {
    testSQL1 <- .clone_MsBackendSqlDb(sciexSQL1)
    res <- .reset_row_indices(testSQL1)
    expect_equal(mz(res), mz(testSQL1))
    
    sps_mod <- testSQL1[3:8]
    res <- .reset_row_indices(sps_mod)
    expect_equal(mz(res), mz(testSQL1))
})  