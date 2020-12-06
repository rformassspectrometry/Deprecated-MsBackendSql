library("testthat")
library("MsBackendSql")
library("RSQLite")

## Load pre-subsetted test dataset
sciexSubset1 <- system.file("extdata/sciex_subset1.db", 
                             package="MsBackendSql")
sciexSubset2 <- system.file("extdata/sciex_subset2.db", 
                             package="MsBackendSql")
sciexAll <- system.file("extdata/sciex_subsetCombined.db", 
                        package="MsBackendSql")
sciexConn1 <- dbConnect(SQLite(), sciexSubset1)
sciexConn2 <- dbConnect(SQLite(), sciexSubset2)
sciexConn <- dbConnect(SQLite(), sciexAll)

## Create `MsBackendSqlDb` instances for preloaded datasets
sciexSQL1 <- backendInitialize(MsBackendSqlDb(), dbcon = sciexConn1)
sciexSQL2 <- backendInitialize(MsBackendSqlDb(), dbcon = sciexConn2)
sciexCombined <- backendInitialize(MsBackendSqlDb(), dbcon = sciexConn)

## Read back the metadata table from the `MsBackendSqlDb` object
testTbl <- dbReadTable(sciexSQL1@dbcon, "msdata")
testTbl$mz <- lapply(testTbl$mz, 'unserialize') 
testTbl$intensity <- lapply(testTbl$intensity, 'unserialize') 

## Subsetted mzML files
sciexmzML1 <- system.file("extdata/sciex_subset1.mzML", 
                            package="MsBackendSql")
sciexmzML2 <- system.file("extdata/sciex_subset2.mzML", 
                          package="MsBackendSql")
sciexmzMLAll <- c(sciexmzML1, sciexmzML2)

## Create `MsBackendMzR` for test
sciex_mzR1 <- Spectra::backendInitialize(MsBackendMzR(), files = sciexmzML1)
sciex_mzR2 <- Spectra::backendInitialize(MsBackendMzR(), files = sciexmzML2)
sciex_mzR_All <- Spectra::backendInitialize(MsBackendMzR(), files = sciexmzMLAll)

## Create a `MsBackendSql` object with its `dbtable` only has 3
## columns: "acquisitionNum", "intensity", "_pkey" as Primary Key
testSQL1 <- MsBackendSql:::.clone_MsBackendSqlDb(sciexSQL1)
##
res <- dbSendQuery(testSQL1@dbcon, "CREATE TABLE msdata2 AS 
                                    SELECT acquisitionNum, intensity, _pkey
                                    FROM msdata")
res <- dbSendStatement(testSQL1@dbcon, "DROP TABLE IF EXISTS msdata")
res <- dbSendStatement(testSQL1@dbcon, "ALTER TABLE msdata2
                                        RENAME TO msdata")
testSQL1@columns <- c("acquisitionNum", "intensity")

## New test cases by Sebastian 
## His method can reduce the loading size of the package
msdf <- data.frame(
    rtime = c(1.2, 3.4, 5.6),
    msLevel = c(1L, 2L, 2L),
    scanIndex = 4:6,
    dataStorage = "<db>",
    dataOrigin = "file.mzML",
    stringsAsFactors = FALSE
)
msdf$mz <- sciexSQL1$mz[1:3]
msdf$intensity <- sciexSQL1$intensity[1:3]

## Provide a valid DBIConnection from SQLite 
test_con <- dbConnect(SQLite(), "test.db")
on.exit(DBI::dbDisconnect(test_con))
test_con1 <- dbConnect(SQLite(), "test1.db")
on.exit(DBI::dbDisconnect(test_con1))

test_check("MsBackendSql")
