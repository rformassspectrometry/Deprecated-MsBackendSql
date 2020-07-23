test_that("initializeBackend,MsBackendSqlDb works", {
    test_con1 <- dbConnect(SQLite(), "test_con1.db")
  
    be <- MsBackendSqlDb()
    expect_true(is(be, "MsBackendSqlDb"))
    ## initialize `MsBackendSqlDb` from `data`
    be2 <- backendInitialize(be, data = msdf[msdf$msLevel %in% 2L, ])
    expect_true(is(be2, "MsBackendSqlDb"))
    expect_equal(dataStorage(be2), c("<db>", "<db>"))
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
    expect_error(backendInitialize(MsBackendSqlDb(dbcon = test_con1)),
                 "no such table: msdata")
    expect_error(backendInitialize(MsBackendSqlDb(), 
                                   dbcon = test_con1,
                                   files = 4),
                 "invalid 'file' argument")
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

test_that("backendMerge,MsBackendSqlDb works", {
    be <- backendInitialize(MsBackendSqlDb(), data = msdf)
  
    expect_equal(backendMerge(sciexSQL1), sciexSQL1)
    expect_error(backendMerge(sciexSQL1, 4), "backends of the same type")
  
    res <- backendMerge(sciexSQL1, sciexSQL2)
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
})

test_that("Spectra,character works", {
    res <- Spectra(sciexmzML1, backend = MsBackendSqlDb())
    expect_true(is(res@backend, "MsBackendSqlDb"))
    expect_equal(unique(res@backend$dataStorage), sciexmzML1)
    expect_identical(rtime(res), rtime(sciex_mzR1))
  
    show(res)
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
  expect_true(is(isolationWindowLowerMz(test_be), "numeric"))
  expect_identical(isolationWindowLowerMz(test_be), isolationWindowLowerMz(b1))
})

test_that("as.list,MsBackendSqlDb works", {
    expect_identical(as.list(test_be), as.list(b1))
})



test_that("isolationWindowTargetMz,MsBackendSqlDb works", {
    expect_true(is(isolationWindowTargetMz(test_be), "numeric"))
    expect_identical(isolationWindowTargetMz(test_be), 
                     isolationWindowTargetMz(b1))
})

test_that("isolationWindowUpperMz,MsBackendSqlDb works", {
    expect_true(is(isolationWindowUpperMz(test_be), "numeric"))
    expect_identical(isolationWindowUpperMz(test_be), 
                     isolationWindowUpperMz(b1))
})

test_that("length,MsBackendSqlDb works", {
    be <- MsBackendSqlDb()
    expect_equal(length(be), 0)
    expect_equal(length(test_be), nrow(test_tbl))
})

test_that("msLevel,MsBackendSqlDb works", {
    expect_true(is(msLevel(test_be), "integer"))
    expect_identical(msLevel(test_be), msLevel(b1))
})

test_that("mz,MsBackendSqlDb works", {
    expect_true(is(mz(test_be), "NumericList"))
    expect_identical(mz(test_be), mz(b1))
})

test_that("lengths,MsBackendDataFrame works", {
    expect_identical(lengths(test_be), lengths(b1))
    expect_true(is(lengths(test_be), "integer"))
})

test_that("polarity,MsBackendSqlDb works", {
    expect_true(is(polarity(test_be), "integer"))
    expect_identical(polarity(test_be), polarity(b1))
})

test_that("precScanNum,MsBackendSqlDb works", {
    expect_true(is(precScanNum(test_be), "integer"))
    expect_identical(precScanNum(test_be), precScanNum(b1))
})

test_that("precursorCharge,MsBackendSqlDb works", {
    expect_true(is(precursorCharge(test_be), "integer"))
    expect_identical(precursorCharge(test_be), precursorCharge(b1))
})

test_that("precursorIntensity,MsBackendSqlDb works", {
    expect_true(is(precursorIntensity(test_be), "numeric"))
    expect_identical(precursorIntensity(test_be), precursorIntensity(b1))
})

test_that("precursorMz,MsBackendSqlDb works", {
    expect_true(is(precursorMz(test_be), "numeric"))
    expect_identical(precursorMz(test_be), precursorMz(b1))
})

test_that("rtime,MsBackendSqlDb works", {
    expect_true(is(rtime(test_be), "numeric"))
    expect_identical(rtime(test_be), rtime(b1))
})

test_that("scanIndex,MsBackendSqlDb works", {
    expect_true(is(scanIndex(test_be), "integer"))
    expect_identical(scanIndex(test_be), scanIndex(b1))
})

test_that("smoothed,MsBackendSqlDb works", {
    expect_true(is(smoothed(test_be), "logical"))
    expect_identical(smoothed(test_be), smoothed(b1))
})

test_that("tic,MsBackendSqlDb works", {
    expect_true(is(tic(test_be), "numeric"))
    expect_identical(tic(test_be, initial = TRUE), tic(b2, initial = TRUE))
    expect_identical(tic(test_be, initial = FALSE), tic(b2, initial = FALSE))
})

test_that("spectraVariables,MsBackendSqlDb works", {
    expect_equal(spectraVariables(test_be), spectraVariables(b1))
})

test_that("$,MsBackendSqlDb works", {
    expect_true(is(test_be$msLevel, "integer"))
    expect_identical(test_be$msLevel, b1$msLevel)
    expect_identical(test_be$intensity, b1$intensity)
})

test_that("Spectra,character works", {
    conn1 <- dbConnect(SQLite(), "sciex.db")
    on.exit(DBI::dbDisconnect(conn1))
    res <- Spectra(sciex_file, backend = MsBackendSqlDb(conn1))
    expect_true(is(res@backend, "MsBackendSqlDb"))
    expect_equal(unique(res@backend$dataStorage), "<db>")
    expect_identical(rtime(res), rtime(sciex_mzr))
    show(res)
})

test_that("Spectra,MsBackendSqlDb works", {
    conn2 <- dbConnect(SQLite(), "sciex.db")
    on.exit(DBI::dbDisconnect(conn2))
    res <- Spectra(MsBackendSqlDb(conn2))
    expect_true(length(res) == length(sciex_mzr))
    expect_identical(mz(res), mz(sciex_mzr))
    expect_true(is(res@backend, "MsBackendSqlDb"))
    show(res)
})