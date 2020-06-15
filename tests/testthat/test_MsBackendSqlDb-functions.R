test_con <- dbConnect(SQLite(), "test2.db")
test_con1 <- dbConnect(SQLite(), "test3.db")
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
                        pkey = test_tbl$X_pkey))
dbExecute(test_con1,
          "ALTER TABLE msdata3 RENAME TO temp_msdata3;")
dbExecute(test_con1, "CREATE TABLE msdata3 (
	                  acquisitionNum INT,
	                  intensity BLOB,
	                  _pkey INT PRIMARY KEY);")
dbExecute(test_con1, "INSERT INTO msdata3(acquisitionNum, intensity, _pkey)
	                  SELECT acquisitionNum, intensity, pkey
                      FROM temp_msdata3;")
dbExecute(test_con1, "DROP TABLE temp_msdata3;")

tmp_tbl <- test_tbl
tmp_tbl[, c("intensity", "mz")] <- NULL
dbWriteTable(test_con1, "msdata4", tmp_tbl)
dbExecute(test_con1,
          "ALTER TABLE msdata4 RENAME TO temp_msdata4;")
type4 <- dbDataType(test_con1, tmp_tbl)[-length(tmp_tbl)]
str4 <- paste(names(type4), type4, sep = " ")
str4 <- paste(str4, collapse=', ')
sql4 <- paste("CREATE TABLE msdata4 (",
              str4,
              ", _pkey INT PRIMARY KEY);", sep = "")
dbExecute(test_con1, sql4)
sql4_1 <- paste("INSERT INTO msdata4(", 
                paste(names(type4), collapse = ", "),
                ", _pkey) ",
                "SELECT * FROM temp_msdata4;")
dbExecute(test_con1, "DROP TABLE temp_msdata4;")

test_tbl1 <- test_tbl
test_tbl1[, c("intensity", "mz")] <- 1
dbWriteTable(test_con1, "msdata5", test_tbl1)
dbExecute(test_con1,
          "ALTER TABLE msdata5 RENAME TO temp_msdata5;")
type5 <- dbDataType(test_con1, test_tbl1)[-length(test_tbl1)]
str5 <- paste(names(type5), type5, sep = " ")
str5 <- paste(str5, collapse=', ')
sql5 <- paste("CREATE TABLE msdata5 (",
              str5,
              ", _pkey INT PRIMARY KEY);", sep = "")
dbExecute(test_con1, sql5)
sql5_1 <- paste("INSERT INTO msdata5(", 
                paste(names(type5), collapse = ", "),
                ", _pkey) ",
                "SELECT * FROM temp_msdata5;")
dbExecute(test_con1, "DROP TABLE temp_msdata5;")


test_that(".valid_db_table_has_columns works", {
    expect_error(.valid_db_table_columns(test_con1))
    expect_match(.valid_db_table_columns(test_con, "msdata", "random"),
                 "required column 'random' not found")
    expect_identical(.valid_db_table_columns(test_con, "msdata2"),
                 "database table 'msdata2' not found")
    expect_match(.valid_db_table_columns(test_con1, "msdata2"),
                 "required column '_pkey' not found")
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

