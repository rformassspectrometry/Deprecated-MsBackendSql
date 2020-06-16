test_con <- dbConnect(SQLite(), "test.db")
test_con1 <- dbConnect(SQLite(), "test1.db")
dbExecute(test_con, "PRAGMA auto_vacuum = FULL")
dbExecute(test_con1, "PRAGMA auto_vacuum = FULL")
fl <- msdata::proteomics(full.names = TRUE)[4]
test_be <- backendInitialize(MsBackendSqlDb(), test_con, fl)
b1 <- Spectra::backendInitialize(MsBackendMzR(), files = fl)
b2 <- Spectra(fl, backend = MsBackendMzR())
b2 <- setBackend(b2, MsBackendDataFrame())
test_tbl <- dbReadTable(test_con, "msdata")

test_that("initializeBackend,MsBackendSqlDb works", {
    expect_error(backendInitialize(MsBackendSqlDb()))
    expect_error(backendInitialize(MsBackendSqlDb(), test_con1),
                 "no such table: msdata")
    expect_error(backendInitialize(MsBackendSqlDb(), fl))
    expect_error(backendInitialize(MsBackendSqlDb(), 
                                   test_con,
                                   files = 4),
                 "invalid 'file' argument")
    expect_equal(dataStorage(test_be)[1], "<db>")
    expect_true(is(mz(test_be), "NumericList"))
    expect_true(is(intensity(test_be), "NumericList"))
})

test_that("as.list,MsBackendSqlDb works", {
    expect_identical(as.list(test_be), as.list(b1))
})

test_that("acquisitionNum, MsBackendSqlDb works", {
    expect_equal(acquisitionNum(test_be), acquisitionNum(b1))
    expect_true(is(acquisitionNum(test_be), "integer"))
    expect_equal(acquisitionNum(test_be), 1:7534)
})

test_that("collisionEnergy, MsBackendSqlDb work", {
    expect_equal(collisionEnergy(test_be), collisionEnergy(b1))
    expect_true(is(collisionEnergy(test_be), "numeric"))
})

test_that("centroided, MsBackendSqlDb work", {
  expect_true(is(centroided(test_be), "logical"))
  expect_true(!all(centroided(test_be)))
})

test_that("dataOrigin,MsBackendSqlDb works", {
    expect_equal(gsub("\\\\", "/", dataOrigin(test_be)), 
                  gsub("\\\\", "/", dataOrigin(b1)))
    expect_true(is(dataOrigin(test_be), "character"))
})

test_that("dataStorage,MsBackendSqlDb works", {
    expect_true(is(dataStorage(test_be), "character"))
    expect_identical(dataStorage(test_be), rep("<db>", 7534))
})

test_that("intensity,MsBackendSqlDb works", {
    expect_true(is(intensity(test_be), "NumericList"))
    expect_identical(intensity(test_be), intensity(b1))
})

test_that("ionCount,MsBackendSqlDb works", {
    expect_identical(ionCount(test_be), ionCount(b1))
    expect_true(is(ionCount(test_be), "numeric"))
})

test_that("isolationWindowLowerMz,MsBackendSqlDb works", {
    expect_true(is(isolationWindowLowerMz(test_be), "numeric"))
    expect_identical(isolationWindowLowerMz(test_be), isolationWindowLowerMz(b1))
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