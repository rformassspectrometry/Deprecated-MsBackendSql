library("testthat")
library("MsBackendSql")
library("Spectra")
library("RSQLite")
library("dplyr")
library("msdata")
library("MsCoreUtils")

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

tmp_tbl <- dplyr::select(test_tbl, -c(intensity, mz))
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

test_tbl1 <- test_tbl %>% dplyr::mutate(intensity = 1, mz = 1)
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

test_check("MsBackendSql")
