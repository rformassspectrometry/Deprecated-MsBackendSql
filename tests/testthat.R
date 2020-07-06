library("testthat")
library("MsBackendSql")
library("RSQLite")


## Provide a valid DBIConnection from SQLite 
test_con <- dbConnect(SQLite(), "test.db")
on.exit(DBI::dbDisconnect(test_con))
test_con1 <- dbConnect(SQLite(), "test1.db")
on.exit(DBI::dbDisconnect(test_con1))

## Activate `VACUUM` mode of SQLite DB,
dbExecute(test_con, "PRAGMA auto_vacuum = FULL")
dbExecute(test_con1, "PRAGMA auto_vacuum = FULL")

## Proivde raw `mzML` files for unit tests
fl <- msdata::proteomics(full.names = TRUE, 
                         pattern = "TMT_Erwinia_1uLSike_Top10HCD_isol2_45stepped_60min_01-20141210.mzML.gz")
sciex_file <- dir(system.file("sciex", package = "msdata"), full.names = TRUE)

## Initialize a `MsBackendMzR` object containing `sciex_file`
sciex_mzr <- backendInitialize(MsBackendMzR(), files = sciex_file)

## Initialize a `MsBackendSqlDb` for test 
test_be <- backendInitialize(MsBackendSqlDb(), test_con, fl)

## Create several `Spectra` objects with different backends
b1 <- Spectra::backendInitialize(MsBackendMzR(), files = fl)
b2 <- Spectra(fl, backend = MsBackendMzR())
b2 <- setBackend(b2, MsBackendDataFrame())

## Read back the metadata table from the `MsBackendSqlDb` object
test_tbl <- dbReadTable(test_con, "msdata")


## Provide a valid DBIConnection from SQLite 
test_con2 <- dbConnect(SQLite(), "test2.db")
on.exit(DBI::dbDisconnect(test_con2))
test_con3 <- dbConnect(SQLite(), "test3.db")
on.exit(DBI::dbDisconnect(test_con3))

## Activate `VACUUM` mode of SQLite DB,
dbExecute(test_con2, "PRAGMA auto_vacuum = FULL")
dbExecute(test_con3, "PRAGMA auto_vacuum = FULL")

## Initialize a `MsBackendSqlDb` for test 
test_be2 <- backendInitialize(MsBackendSqlDb(), test_con2, fl)

## Create several `Spectra` objects with different backends
sps_b1 <- Spectra(fl, backend = MsBackendMzR())

## Read back the metadata table from the `MsBackendSqlDb` object
test_tbl <- dbReadTable(test_con2, "msdata")
## Create a SQLite dbtable with only 2 columns: acquisitionNum, polarity
## This table is used to mimic a `MsBackendSqlDb` obj having missing columns
dbWriteTable(test_con3, "msdata2", 
             data.frame(acquisitionNum = test_tbl$acquisitionNum, 
                        polarity= test_tbl$polarity))
## Create another SQLite dbtable with missing columns but having a primary key
dbWriteTable(test_con3, "msdata3", 
             data.frame(acquisitionNum = test_tbl$acquisitionNum, 
                        intensity = test_tbl$intensity,
                        pkey = test_tbl$X_pkey))
## The following SQL statement are used to rename `pkey` to `_pkey`
dbExecute(test_con3,
          "ALTER TABLE msdata3 RENAME TO temp_msdata3;")
dbExecute(test_con3, "CREATE TABLE msdata3 (
	                  acquisitionNum INT,
	                  intensity BLOB,
	                  _pkey INT PRIMARY KEY);")
dbExecute(test_con3, "INSERT INTO msdata3(acquisitionNum, intensity, _pkey)
	                  SELECT acquisitionNum, intensity, pkey
                      FROM temp_msdata3;")
dbExecute(test_con3, "DROP TABLE temp_msdata3;")

## Create a data frame without `intensity` and `mz` columns
tmp_tbl <- test_tbl
tmp_tbl[, c("intensity", "mz")] <- NULL

## We write this table into SQLite DB as `msdata4`
dbWriteTable(test_con3, "msdata4", tmp_tbl)
dbExecute(test_con3,
          "ALTER TABLE msdata4 RENAME TO temp_msdata4;")

## The following SQL statement are used to rename `pkey` to `_pkey`
## To create a new table and insert all the columns from old table
## We need to retrieve the data types of each column
type4 <- dbDataType(test_con3, tmp_tbl)[-length(tmp_tbl)]
str4 <- paste(names(type4), type4, sep = " ")
str4 <- paste(str4, collapse=', ')
sql4 <- paste("CREATE TABLE msdata4 (",
              str4,
              ", _pkey INT PRIMARY KEY);", sep = "")
dbExecute(test_con3, sql4)
sql4_1 <- paste("INSERT INTO msdata4(", 
                paste(names(type4), collapse = ", "),
                ", _pkey) ",
                "SELECT * FROM temp_msdata4;")
dbExecute(test_con3, sql4_1)
dbExecute(test_con3, "DROP TABLE temp_msdata4;")

## We create another SQL data table, with all intenisy and mz values as 1
## The SQLite db table is named as `msdata5`
test_tbl1 <- test_tbl
test_tbl1[, c("intensity", "mz")] <- 1
dbWriteTable(test_con3, "msdata5", test_tbl1)
## We rename the primary key in `msdata5` as `_pkey`
dbExecute(test_con3,
          "ALTER TABLE msdata5 RENAME TO temp_msdata5;")
type5 <- dbDataType(test_con3, test_tbl1)[-length(test_tbl1)]
str5 <- paste(names(type5), type5, sep = " ")
str5 <- paste(str5, collapse=', ')
sql5 <- paste("CREATE TABLE msdata5 (",
              str5,
              ", _pkey INT PRIMARY KEY);", sep = "")
dbExecute(test_con3, sql5)
sql5_1 <- paste("INSERT INTO msdata5(", 
                paste(names(type5), collapse = ", "),
                ", _pkey) ",
                "SELECT * FROM temp_msdata5;")
dbExecute(test_con3, sql5_1)
dbExecute(test_con3, "DROP TABLE temp_msdata5;")

rm(tmp_tbl, test_tbl1)

## New test cases by Sebastian 
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


test_check("MsBackendSql")
