##  Script used to subset sciex mzML files from `msdata`, only first 10 rows 
##      are kept in the tiny mzML output.
##
##  Author: Chong Tang

library("msdata")
library("mzR")
library("RSQLite")
library("MsBackendSql")


## Fetch the path of sciex files (mzML format)
sciex_file_1 <- dir(system.file("sciex", package = "msdata"), full.names = TRUE,
                    pattern = "POS_1_105-134.mzML")
sciex_file_2 <- dir(system.file("sciex", package = "msdata"), full.names = TRUE,
                    pattern = "POS_3_105-134.mzML")

## From `mzR` manuscript
## Author: Johannes Rainer
## open the 1st mzML file
ms_fl <- openMSfile(sciex_file_1, backend = "pwiz")

## Get the spectra
pks <- spectra(ms_fl)

## Get the header
hdr <- header(ms_fl)

## Subset both spectra and header data, keep only the first 10 rows, respectively
pks <- pks[1:10]
hdr <- hdr[1:10, ]

## Write the data to a mzML file.
out_file <- "../extdata/sciex_subset1.mzML"
writeMSData(object = pks, file = out_file, header = hdr)


## Then use the same procedure to subset the 2nd mzML file
ms_fl <- openMSfile(sciex_file_2, backend = "pwiz")
pks <- spectra(ms_fl)
hdr <- header(ms_fl)
pks <- pks[1:10]
hdr <- hdr[1:10, ]
out_file <- "../extdata/sciex_subset2.mzML"
writeMSData(object = pks, file = out_file, header = hdr)



## We can also create small SQLite files for the 2 subsetted files
conn1 <- dbConnect(SQLite(), "../extdata/sciex_subset1.db")
conn2 <- dbConnect(SQLite(), "../extdata/sciex_subset2.db")
sciex_be1 <- backendInitialize(MsBackendSqlDb(conn1),
                               files = normalizePath("../extdata/sciex_subset1.mzML"))
sciex_be2 <- backendInitialize(MsBackendSqlDb(conn2),  
                               files = normalizePath("../extdata/sciex_subset2.mzML"))

## Now we want to create a MsBackendSqlDb instance by using 
## these 2 small subsetted mzML files
## Fetch the path of sciex files (mzML format)
sciex_file <- dir(system.file("extdata", package = "MsBackendSql"), 
                  full.names = TRUE, pattern = ".mzML")
connCombined <- dbConnect(SQLite(), "../extdata/sciex_subsetCombined.db")
sciexCombined <- backendInitialize(MsBackendSqlDb(connCombined),  
                               files = sciex_file)

