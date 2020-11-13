test_that("initializeBackend,MsBackendSqlDb works", {
    test_con1 <- dbConnect(SQLite(), "test_con1.db")
  
    be <- MsBackendSqlDb()
    expect_true(is(be, "MsBackendSqlDb"))
    ## initialize `MsBackendSqlDb` from `data`
    df <- DataFrame(testTbl)
    df$dataStorage <- c(rep(c("fake", "backend", "dataStorage"), 3), "none")
    be3 <- backendInitialize(MsBackendSqlDb(), 
                             data = df)
    expect_identical(be3$dataStorage, rep("<db>", 10))
    expect_identical(dataStorage(be3), rep("<db>", 10))
    
    msdf$mz <- testTbl$mz[5:7]
    msdf$intensity <- testTbl$intensity[5:7]
    be2 <- backendInitialize(be, data = msdf[msdf$msLevel %in% 2L, ])
    expect_true(is(be2, "MsBackendSqlDb"))
    
    ## Read back `mz` and `intensity` columns as `NumericList`
    expect_true(is(be2$mz, "NumericList"))
    expect_true(is(be2$intensity, "NumericList"))

    ## initialize `MsBackendSqlDb` from file path (mzML)
    be1 <- backendInitialize(MsBackendSqlDb(), sciexmzML1)
    expect_true(is(be1, "MsBackendSqlDb"))
    ## 
    expect_error(backendInitialize(be, data = 4),
                 "is supposed to be a")
    
    
    expect_error(backendInitialize(MsBackendSqlDb()))
    expect_error(backendInitialize(MsBackendSqlDb(), dbcon = test_con1),
                 "no such table: msdata")
    expect_error(backendInitialize(MsBackendSqlDb(), 
                                   dbcon = test_con1,
                                   files = 4),
                 "invalid 'file' argument")
    ## sciexSQL1 was initialized using `sciex_subset1.mzML` (on-disk file)
    expect_identical(dataStorage(sciexSQL1)[1], "<db>")
    expect_true(is(mz(sciexSQL1), "NumericList"))
    expect_true(is(intensity(sciexSQL1), "NumericList"))
})

test_that("show,MsBackendSqlDb works", {
    be <- MsBackendSqlDb()
    show(be)
    df <- DataFrame(msdf)
    be <- backendInitialize(be, data = df)
    show(be)
})

test_that("Spectra,character works", {
    conn <- dbConnect(SQLite(), "msdata.db")
    res <- Spectra(sciexmzMLAll, backend = MsBackendSqlDb(dbcon = conn),
                                 BPPARAM = SerialParam())
    expect_true(is(res@backend, "MsBackendSqlDb"))
    expect_equal(unique(res@backend$dataStorage), 
                 "<db>")
    expect_equal(unique(res@backend$dataOrigin), 
                 normalizePath(sciexmzMLAll, winslash = "/"))
    expect_identical(rtime(res), rtime(sciex_mzR_All))
    res_2 <- Spectra(sciexmzMLAll)
    expect_identical(rtime(res), rtime(res_2))
  
    show(res)
})

test_that("backendMerge,MsBackendSqlDb works", {
    be <- backendInitialize(MsBackendSqlDb(), data = msdf)
    
    ## Test for conditions:
    expect_equal(backendMerge(sciexSQL1), sciexSQL1)
    expect_error(backendMerge(sciexSQL1, 4), "backends of the same type")
  
    ## Test case 1: when `dbcon` isn't provided
    testSQL1 <- .clone_MsBackendSqlDb(sciexSQL1)
    res <- backendMerge(testSQL1, sciexSQL2)
    expect_true(is(res, "MsBackendSqlDb"))
    expect_identical(res$dataStorage, rep("<db>", 20))
    expect_identical(dataStorage(res), rep("<db>", 20))
    expect_identical(msLevel(res), msLevel(sciexCombined))
    expect_identical(rtime(res), rtime(sciexCombined))
    expect_true(is(res$precScanNum, "integer"))
    expect_true(is(res$mz, "NumericList"))
    expect_true(is(res$intensity, "SimpleNumericList"))
    expect_identical(res$mz, sciexCombined$mz)
    expect_identical(res$intensity, sciexCombined$intensity)
    expect_identical(res$rtime, sciexCombined$rtime)
    expect_identical(res$msLevel, sciexCombined$msLevel)
    expect_identical(res@dbtable, sciexCombined@dbtable)
    expect_identical(res@dbcon, testSQL1@dbcon)
    rm(res, testSQL1)
    
    ## Extra test with Spectra constructor
    ## Also the test case, while `dbcon` isn't provided.
    connTest <- dbConnect(SQLite(), "tmpMerge1.db")
    sp_test <- Spectra(sciexmzMLAll, 
                       backend = MsBackendSqlDb(dbcon = connTest),
                       BPPARAM = SerialParam())
    expect_identical(sp_test@backend@dbtable, dbListTables(connTest))
    expect_identical(sp_test@backend@dbcon, connTest)
    expect_identical(sp_test$mz, sciexCombined$mz)
    expect_identical(sp_test$intensity, sciexCombined$intensity)
    expect_identical(sp_test$rtime, sciexCombined$rtime)
    expect_identical(sp_test$msLevel, sciexCombined$msLevel)
    expect_identical(nrow(dbReadTable(sp_test@backend@dbcon, 
                                      sp_test@backend@dbtable)), 20L)
    
    ## Test case 2:
    testSQL1 <- .clone_MsBackendSqlDb(sciexSQL1)
    connTest2 <- dbConnect(SQLite(), "tmp2.db")
    res <- backendMerge(testSQL1, sciexSQL2, dbcon = connTest2)
    expect_identical(res@dbcon, connTest2)
    expect_identical(dbListTables(connTest2), res@dbtable)
    expect_identical(nrow(dbReadTable(res@dbcon, 
                                      res@dbtable)), 20L)
    expect_identical(res$mz, sciexCombined$mz)
    expect_identical(res$intensity, sciexCombined$intensity)
    expect_identical(res$rtime, sciexCombined$rtime)
    expect_identical(res$msLevel, sciexCombined$msLevel)
    ## And the function doesn't have impact over testSQL1
    expect_identical(nrow(dbReadTable(testSQL1@dbcon, 
                                      testSQL1@dbtable)), 10L)
    expect_identical(testSQL1@dbtable, sciexSQL1@dbtable)
    expect_identical(identical(testSQL1@dbcon, res@dbcon), FALSE)
    expect_identical(spectraData(testSQL1), spectraData(sciexSQL1))
})

test_that("acquisitionNum, MsBackendSqlDb works", {
    expect_equal(acquisitionNum(sciexSQL1), acquisitionNum(testSQL1))
    expect_true(is(acquisitionNum(sciexSQL1), "integer"))
  
    be <- MsBackendSqlDb()
    be <- backendInitialize(be, data = msdf)
    expect_match(acquisitionNum(be),
                 "Columns missing from database.")
    be <- MsBackendSqlDb()
    be <- backendInitialize(be, data = DataFrame(msdf,
                                                 acquisitionNum = 1:3))
    expect_equal(acquisitionNum(be), 1:3)
})

test_that("centroided, MsBackendSqlDb work", {
    expect_true(is(centroided(sciexSQL1), "logical"))
    
    be <- MsBackendSqlDb()
    be <- backendInitialize(be, data = msdf)
    expect_identical(centroided(be), NA)
    be <- MsBackendSqlDb()
    be <- backendInitialize(be, data = DataFrame(msdf,
                        centroided = as.logical(c("TRUE", "FALSE", "TRUE"))))
    expect_equal(centroided(be), c(TRUE, FALSE, TRUE))
})

test_that("collisionEnergy, MsBackendSqlDb work", {
    expect_true(is(collisionEnergy(sciexSQL1), "integer"))
  
    be <- MsBackendSqlDb()
    be <- backendInitialize(be, data = msdf)
    expect_match(collisionEnergy(be),
                 "Columns missing from database.")
    be <- MsBackendSqlDb()
    be <- backendInitialize(be, data = DataFrame(msdf,
                                                 collisionEnergy = c(.5, 2.3, 6)))
    expect_equal(collisionEnergy(be), c(.5, 2.3, 6))
})

test_that("dataOrigin,MsBackendSqlDb works", {
    expect_true(is(dataOrigin(sciexSQL1), "character"))
  
    be <- MsBackendSqlDb()
    be <- backendInitialize(be, data = msdf)
    expect_equal(dataOrigin(be), c("file.mzML", "file.mzML", "file.mzML"))
})

test_that("dataStorage,MsBackendSqlDb works", {
    expect_true(is(dataStorage(sciexSQL1), "character"))
    
    be <- MsBackendSqlDb()
    be <- backendInitialize(be, data = msdf)
    expect_identical(dataStorage(be), rep("<db>", 3))
})

test_that("intensity,MsBackendSqlDb works", {
    expect_true(is(intensity(sciexSQL1), "NumericList"))
  
    be <- MsBackendSqlDb()
    be <- backendInitialize(be, data = msdf)
    expect_true(is(intensity(be), "SimpleNumericList"))
    expect_equal(intensity(be), sciexSQL1$intensity[1:3])
})

test_that("ionCount,MsBackendSqlDb works", {
    expect_true(is(ionCount(sciexSQL1), "numeric"))
  
    df <- DataFrame(msdf)
    df$intensity <- list(1:4, c(2.1, 3.4), c(6.7, 101.9, 5.7))
    df$mz <- list(1:4, 1:2, 7:11)
    be <- MsBackendSqlDb()
    be <- backendInitialize(be, data = df)
    expect_equal(ionCount(be), c(sum(1:4), sum(c(2.1, 3.4)),
                                 sum(6.7, 101.9, 5.7)))
})

test_that("isEmpty,MsBackendSqlDb works", {
    df <- DataFrame(msdf)
    df$intensity <- list(numeric(0), numeric(0), numeric(0))
    df$mz <- list(NA, rep(0, 9), rep(0, 5))
    be <- MsBackendSqlDb()
    be <- backendInitialize(be, data = df)
    expect_equal(isEmpty(be), rep(TRUE, 3))
    
    df$intensity <- list(1:2, 1:5, numeric(0))
    be <- MsBackendSqlDb()
    be <- backendInitialize(be, data = df)
    expect_equal(isEmpty(be), c(FALSE, FALSE, TRUE))
})

test_that("isolationWindowLowerMz,MsBackendSqlDb works", {
    df <- DataFrame(msdf)
    be <- MsBackendSqlDb()
    be <- backendInitialize(be, data = df)
    expect_match(isolationWindowLowerMz(be),
                 "Columns missing from database.")
    
    df$isolationWindowLowerMz <- c(NA_real_, 2, 3)
    be <- MsBackendSqlDb()
    be <- backendInitialize(be, data = df)
    expect_identical(isolationWindowLowerMz(be), c(NA_real_, 2, 3))
    expect_true(is(isolationWindowLowerMz(be), "numeric"))
})

test_that("isolationWindowTargetMz,MsBackendSqlDb works", {
    df <- DataFrame(msdf)
    be <- MsBackendSqlDb()
    be <- backendInitialize(be, data = df)
    expect_match(isolationWindowTargetMz(be),
                 "Columns missing from database.")
    df$isolationWindowTargetMz <- c(NA_real_, 2, 3)
    be <- MsBackendSqlDb()
    be <- backendInitialize(be, data = df)
    expect_identical(isolationWindowTargetMz(be), c(NA_real_, 2, 3))
    expect_true(is(isolationWindowTargetMz(be), "numeric"))
})

test_that("isolationWindowUpperMz,MsBackendSqlDb works", {
    df <- DataFrame(msdf)
    be <- MsBackendSqlDb()
    be <- backendInitialize(be, data = df)
    expect_match(isolationWindowUpperMz(be),
                 "Columns missing from database.")
    
    df$isolationWindowUpperMz <- c(NA_real_, 2, 3)
    be <- MsBackendSqlDb()
    be <- backendInitialize(be, data = df)
    expect_identical(isolationWindowUpperMz(be), c(NA_real_, 2, 3))
    expect_true(is(isolationWindowUpperMz(be), "numeric"))
})

test_that("length,MsBackendSqlDb works", {
    df <- DataFrame(msdf)
    be <- MsBackendSqlDb()
    be <- backendInitialize(be, data = df)
    expect_equal(length(be), 3)
    expect_equal(length(sciexSQL1), 10)
})

test_that("msLevel,MsBackendSqlDb works", {
    df <- DataFrame(msdf)
    be <- MsBackendSqlDb()
    be <- backendInitialize(be, data = df)
    expect_equal(msLevel(be), c(1L, 2L, 2L))
    expect_true(is(msLevel(be), "integer"))
    
    df$msLevel <- NULL
    be <- MsBackendSqlDb()
    expect_error(backendInitialize(be, data = df),
                 "msLevel not found")
})

test_that("mz,MsBackendSqlDb works", {
    df <- DataFrame(msdf)
    be <- MsBackendSqlDb()
    be <- backendInitialize(be, data = df)
    expect_equal(mz(be), sciexSQL1$mz[1:3])
    
    df$mz <- NULL
    expect_error(backendInitialize(MsBackendSqlDb(), data = df),
                 "mz not found")
})

test_that("peaksData,MsBackendSqlDb works", {
    expect_true(is(peaksData(sciexSQL1), "list"))
  
    df <- DataFrame(msdf)
    df$mz <- list(1:3, c(2.1), c(3.5, 6.7, 12.3))
    df$intensity <- list(1:3, 4, c(6, 15, 19))
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    expect_equal(peaksData(be), list(cbind(mz = 1:3, intensity = 1:3),
                                   cbind(mz = 2.1, intensity = 4),
                                   cbind(mz = c(3.5, 6.7, 12.3),
                                         intensity = c(6, 15, 19))))
})

test_that("lengths,MsBackendSqlDb works", {
    expect_true(is(lengths(sciexSQL1), "integer"))
  
    df <- DataFrame(msdf)
    df$mz <- list(1:3, c(2.1), c(3.5, 6.7, 12.3))
    df$intensity <- list(1:3, 4, c(6, 15, 19))
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    expect_equal(lengths(be), c(3L, 1L, 3L))
    
    df$mz <- list(numeric(0), numeric(0), numeric(0))
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    expect_equal(lengths(be), c(0, 0, 0))
})

test_that("spectraData,MsBackendSqlDb works", {
    be <- .clone_MsBackendSqlDb(sciexSQL1)
    res <- spectraData(be)
    expect_identical(res$msLevel, testTbl$msLevel)
    expect_identical(res$mz, NumericList(testTbl$mz))
    expect_identical(res$intensity, NumericList(testTbl$intensity))
})

test_that("polarity,MsBackendSqlDb works", {
    expect_true(is(polarity(sciexSQL1), "integer"))
  
    df <- DataFrame(msdf)
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    expect_match(polarity(be), "Columns missing from database.")
    
    df$polarity <- 5:7
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    expect_equal(polarity(be), 5:7)
})

test_that("precScanNum,MsBackendSqlDb works", {
    expect_true(is(precScanNum(sciexSQL1), "integer"))
  
    df <- DataFrame(msdf)
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    expect_match(precScanNum(be), "Columns missing from database.")
    
    df$precScanNum <- c(5L, 1L, 7L)
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    expect_equal(precScanNum(be), c(5L, 1L, 7L))
})

test_that("precursorCharge,MsBackendSqlDb works", {
    expect_true(is(precursorCharge(sciexSQL1), "integer"))
  
    df <- DataFrame(msdf)
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    expect_match(precursorCharge(be), "Columns missing from database.")
    
    df$precursorCharge <- c(-1L, 1L, 0L)
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    expect_equal(precursorCharge(be), c(-1L, 1L, 0L))
})

test_that("precursorIntensity,MsBackendSqlDb works", {
    expect_true(is(precursorCharge(sciexSQL1), "numeric"))
  
    df <- DataFrame(msdf)
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    expect_match(precursorIntensity(be), "Columns missing from database.")
  
    df$precursorIntensity <- c(134.4, 4322.2, 862.54)
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    expect_equal(precursorIntensity(be), c(134.4, 4322.2, 862.54))
})

test_that("precursorMz,MsBackendSqlDb works", {
    expect_true(is(precursorMz(sciexSQL1), "numeric"))
  
    df <- DataFrame(msdf)
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    expect_match(precursorMz(be), "Columns missing from database.")
  
    df$precursorMz <- c(134.4, 342.2, 862.54)
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    expect_equal(precursorMz(be), c(134.4, 342.2, 862.54))
})

test_that("reset,MsBackendSqlDb works", {
    testSQL1 <- .clone_MsBackendSqlDb(sciexSQL1)
    res <- reset(testSQL1)
    expect_equal(mz(res), mz(testSQL1))
  
    sps_mod <- testSQL1[3:8]
    res <- reset(sps_mod)
    expect_equal(mz(res), mz(testSQL1))
})

test_that("rtime,MsBackendSqlDb works", {
    expect_true(is(rtime(sciexSQL1), "numeric"))
  
    df <- DataFrame(msdf)
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    expect_equal(rtime(be), c(1.2, 3.4, 5.6))
  
    df$rtime <- c(123, 124, 125)
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    expect_equal(rtime(be), c(123, 124, 125))
})


test_that("scanIndex,MsBackendSqlDb works", {
    expect_true(is(scanIndex(sciexSQL1), "integer"))
  
    df <- DataFrame(msdf)
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    expect_equal(scanIndex(be), c(4, 5, 6))
  
    df$scanIndex <- c(123, 124, 125)
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    expect_equal(scanIndex(be), c(123, 124, 125))
})


test_that("smoothed,MsBackendSqlDb works", {
    ## smoothed(sciexSQL1) returns "NA" values
    expect_true(is(smoothed(sciexSQL1), "logical"))
  
    df <- DataFrame(msdf)
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    expect_match(smoothed(be), "Columns missing from database.")
  
    df$smoothed <- c(1, 0, 1)
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    expect_equal(smoothed(be), c(TRUE, FALSE, TRUE))
    
    df$smoothed <- c(FALSE, FALSE, TRUE)
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    expect_equal(smoothed(be), c(FALSE, FALSE, TRUE))
})

test_that("spectraNames,MsBackendSqlDb works", {
    df <- DataFrame(msdf)
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    ## This method is explicitly defined to return `NULL`
    expect_null(spectraNames(be))
})

test_that("spectraVariables,MsBackendDataFrame works", {
    df <- DataFrame(msdf)
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    expect_equal(spectraVariables(be), names(Spectra:::.SPECTRA_DATA_COLUMNS))
  
    df$other_column <- 3
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    expect_equal(spectraVariables(be),
                 c(names(Spectra:::.SPECTRA_DATA_COLUMNS), "other_column"))
})

test_that("tic,MsBackendDataFrame works", {
    df <- DataFrame(msdf)
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    expect_true(is(tic(be), "numeric"))
    expect_equal(tic(be), c(NA_real_, NA_real_, NA_real_))
    expect_equal(tic(be, initial = FALSE), 
                 tic(sciexSQL1, initial = FALSE)[1:3])
  
    df$totIonCurrent <- c(5, 3, 7)
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    expect_equal(tic(be), c(5, 3, 7))
    expect_equal(tic(be, initial = FALSE), 
                 tic(sciexSQL1, initial = FALSE)[1:3])
    df$intensity <- list(5:7, 1:4, 20:29)
    df$mz <- list(1:3, 1:4, 1:5)
    be <- backendInitialize(MsBackendSqlDb(), data = df)
    expect_equal(tic(be, initial = FALSE), 
                 c(sum(5:7), sum(1:4), sum(20:29)))
})