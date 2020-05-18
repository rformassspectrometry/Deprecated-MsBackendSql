test_con <- dbConnect(SQLite(), tempfile(pattern = "Test", fileext = ".db"))
test_con1 <- dbConnect(SQLite(), tempfile(pattern = "Test", fileext = "1.db"))
dbExecute(test_con, "PRAGMA auto_vacuum = FULL")
dbExecute(test_con1, "PRAGMA auto_vacuum = FULL")
fl <- msdata::proteomics(full.names = TRUE)[4]
test_be <- backendInitialize(MsBackendSqlDb(), test_con, fl)
b1 <- Spectra::backendInitialize(MsBackendMzR(), files = fl)
sps_b1 <- Spectra(fl, backend = MsBackendMzR())
test_tbl <- dbReadTable(test_con, "msdata")
dbWriteTable(test_con1, "msdata2", 
             data.frame(acquisitionNum = test_tbl$acquisitionNum, 
                        polarity= test_tbl$polarity))
dbWriteTable(test_con1, "msdata3", 
             data.frame(acquisitionNum = test_tbl$acquisitionNum, 
                        intensity = test_tbl$intensity,
                        pkey = test_tbl$pkey))
tmp_tbl <- dplyr::select(test_tbl, -c(intensity, mz))
dbWriteTable(test_con1, "msdata4", tmp_tbl)
test_tbl1 <- test_tbl %>% mutate(intensity = 1, mz = 1)
dbWriteTable(test_con1, "msdata5", test_tbl1)

test_that(".valid_db_table_has_columns works", {
    expect_error(.valid_db_table_columns(test_con1))
    expect_match(.valid_db_table_columns(test_con, "msdata", "random"),
                 "required column 'random' not found")
    expect_identical(.valid_db_table_columns(test_con, "msdata2"),
                 "database table 'msdata2' not found")
    expect_match(.valid_db_table_columns(test_con1, 'msdata2'),
                 "required column 'pkey' not found")
    expect_match(.valid_db_table_columns(test_con1, 'msdata3'),
                 "dataStorage, dataOrigin, rtime, msLevel not found")
    expect_match(.valid_db_table_columns(test_con1, 'msdata4'),
                 "required columns 'mz' and 'intensity' not found")
    expect_match(.valid_db_table_columns(test_con1, 'msdata5'),
                 "'mz' and 'intensity' have the wrong data type")
    expect_null(.valid_db_table_columns(test_con, "msdata", "intensity"))
})

test_that(".valid_db_table_has_columns works", {
    expect_error(.valid_db_table_has_columns())
    expect_match(.valid_db_table_has_columns(test_con, "msdata", "random"),
                 "columns random not found in msdata")
    expect_null(.valid_db_table_has_columns(test_con, 
                                            "msdata",
                                            "collisionEnergy"))
})

test_that(".write_data_to_db works", {
    expect_equal(.write_data_to_db(test_tbl[,1:3], test_con1, "test_tbl1"),
                 7534)
})

test_that(".get_db_data works", {
    expect_equal(.get_db_data(test_be, "totIonCurrent"), 
                 test_tbl$totIonCurrent)
    expect_equal(.get_db_data(test_be, "acquisitionNum"), 
                 test_tbl$acquisitionNum)
})

