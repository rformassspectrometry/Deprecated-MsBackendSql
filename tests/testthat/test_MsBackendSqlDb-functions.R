test_con <- dbConnect(SQLite(), "test.db")
test_con1 <- dbConnect(SQLite(), "test1.db")
fl <- msdata::proteomics(full.names = TRUE)[4]
test_be <- backendInitialize(MsBackendSqlDb(), test_con, fl)
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
    expect_match(.valid_db_table_columns(test_con, "msdata2"),
                 "database table '")
    expect_match(.valid_db_table_columns(test_con1, 'msdata2'),
                 "required column 'pkey' not found")
    expect_match(.valid_db_table_columns(test_con1, 'msdata3'),
                 "dataStorage, dataOrigin, rtime, msLevel not found")
    expect_match(.valid_db_table_columns(test_con1, 'msdata4'),
                 "required columns 'mz' and 'intensity' not found")
    expect_match(.valid_db_table_columns(test_con1, 'msdata5'),
                 "'mz' and 'intensity' have the wrong data type")
})

test_that(".valid_db_table_has_columns works", {
    expect_error(.valid_db_table_has_columns())
})