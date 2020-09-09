#' @include hidden_aliases.R
NULL

#' @title SQL database based mass spectrometry data backend
#'
#' @aliases class:MsBackendSqlDb MsBackendSqlDb-class
#'
#' @description
#'
#' The `MsBackendSqlDb` is an [MsBackend] backend that keeps all
#' metadata and spectra data (m/z and intensity values) in a single
#' on-disk database table.  This object has a very low memory
#' footprint because only primary keys of the database table are
#' stored in memory, within the object hence making it ideal for
#' representing very large mass spectrometry data sets on computers
#' with limited resources.
#'
#' @section Creating an `MsBackendSqlDb` instance:
#'
#' Like all [MsBackend()] objects, `MsBackendSqlDb` have to be
#' *initialized* with the `backendInitialize()` method that either
#' fills the object with data from mass spectrometry files provided
#' with the `files` parameter or checks if the pre-generated database
#' contains all required data. A connection to a database (with write
#' access) has to be provided with the `dbcon` parameter which expects
#' a `DBIConnection` returned by a call to [dbConnect()]. To fill the
#' database with MS data files (usually mzML files, but all files
#' types supported by the `mzR` package are allowed), these have to be
#' provided with the `files` parameter. Parameter `dbtable` can be
#' used to optionally specify the database table name that contains,
#' or should contain the data.
#'
#' @param object a `MsBackendSqlDb` object.
#'
#' @param dbcon a `DBIConnection` object to connect to the database.
#'
#' @param files `character` with the file names from which data should
#'     be imported. Only required if the database to which `dbcon`
#'     connects does not already contain the data.
#'
#' @param data For `backendInitialize`: `DataFrame` with spectrum
#'     metadata/data. This parameter can be empty for `MsBackendMzR` backends
#'     but needs to be provided for `MsBackendDataFrame` and `MsBackendSqlDb` 
#'     backends.
#'     
#' @param ... Additional arguments.
#'
#' @param dbtable `character(1)` the name of the database table with
#'     the data.  Defaults to `dbtable = "msdata"`.
#'
#' @section Implementation notes:
#'
#' The `MsBackendSqlDb` defines the following slots which should not
#' be accessed or changed by the user.
#'
#' @slot dbtable A `character(1)` with the name of the database table
#'     (or view) containing the data.
#' 
#' @slot dbcon A `DBIConnection` with the connection to the database.
#' 
#' @slot modCount An `integer()` that keeps track of each database
#'     writing cycle which would invalidate objects pointing to the
#'     same database. This number has to match between the database
#'     and the object.
#' 
#' @slot rows An `integer()` with the indices (primary keys) of the
#'     data.
#' 
#' @slot columns A `character()` containing the names of the columns
#'     stored in the database.
#'
#' @slot `query` A `DBIResults` object containing SQL query against
#'     the backend.
#'
#' @name MsBackendSqlDb
#'
#' @author Johannes Rainer
#'
#' @export MsBackendSqlDb
#'
#' @exportClass MsBackendSqlDb
#'
#' @examples
#'
#' ## Initialize an MsBackendSqlDb filling it with data from mzML files
#' library(msdata)
#' library(RSQLite)
#' con <- dbConnect(SQLite(), tempfile())
#' fls <- dir(system.file("sciex", package = "msdata"), full.names = TRUE,
#'     pattern = "mzML$")
#' msb <- backendInitialize(MsBackendSqlDb(), dbcon = con, files = fls)
NULL

setClass("MsBackendSqlDb",
         contains = "MsBackend",
         slots = c(dbtable = "character",
                   dbcon = "DBIConnection",
                   modCount = "integer",
                   rows = "integer",
                   columns = "character",
                   query = "DBIResult"),
         prototype = prototype(spectraData = "msdata",
                               readonly = FALSE,
                               modCount = 0L,
                               rows = integer(0),
                               columns = character(0),
                               version = "0.1"))

setValidity("MsBackendSqlDb", function(object) {
    msg <- .valid_db_table_exists(object@dbcon, object@dbtable)
    msg <- c(msg, .valid_db_table_columns(object@dbcon, object@columns))
    if (is.null(msg)) TRUE
    else msg
})

#' @importMethodsFrom methods show
#' 
#' @importMethodsFrom S4Vectors head tail
#'
#' @importFrom utils capture.output
#' 
#' @rdname hidden_aliases
setMethod("show", "MsBackendSqlDb", function(object) {
    if (length(object@rows) == 0) {
        cat(class(object), "with", 0, "spectra\n")
  } else if (length(object@rows) > 10) {
    ## get the first 3 and last 3 rows, and print them
    columns <- c("msLevel", "rtime", "scanIndex")
    if (length(setdiff(columns, object@columns)) == 0) {
        qry <- dbSendQuery(object@dbcon,
                           paste0("select msLevel, rtime, scanIndex",
                                " from ", object@dbtable, " where _pkey = ?"))
        rows_print <- c(head(object@rows, 3), 
                        tail(object@rows, 3))
        qry <- dbBind(qry, list(rows_print))
        res <- dbFetch(qry)
        dbClearResult(qry)
        res <- DataFrame(res)
        cat(class(object), "with", length(object@rows), "spectra\n")
        txt <- capture.output(print(res))
        ## Use ellipses to split the head and tails of output string
        txt_prt <- c(txt[-1][1:5],
                     "...     ...       ...       ...",
                     txt[-1][6:8])
        ## Replace the index numbers with correct row numbers
        txt_prt[7] <- gsub("^[0-9]\\s\\s\\s", toString(length(object) - 2), 
                           txt_prt[7])
        txt_prt[8] <- gsub("^[0-9]\\s\\s\\s", toString(length(object) - 1), 
                           txt_prt[8])
        txt_prt[9] <- gsub("^[0-9]\\s\\s\\s", toString(length(object)), 
                           txt_prt[9])
        cat(txt_prt, sep = "\n")
        sp_cols <- spectraVariables(object)
        cat(" ...", length(sp_cols) - 3, "more variables/columns.\n")
    } else {
        return("Columns missing from database.")
      }
    } else {
        spd <- spectraData(object, c("msLevel", "rtime", "scanIndex"))
        cat(class(object), "with", nrow(spd), "spectra\n")
        txt <- capture.output(print(spd))
        cat(txt[-1], sep = "\n")
        sp_cols <- spectraVariables(object)
        cat(" ...", length(sp_cols) - 3, "more variables/columns.\n")
    }
})

#' @rdname MsBackendSqlDb
#'
#' @importMethodsFrom Spectra backendInitialize
#'
#' @importFrom DBI dbIsValid dbGetQuery
#'
#' @exportMethod backendInitialize
setMethod("backendInitialize", signature = "MsBackendSqlDb",
          function(object, files = character(), data = DataFrame(), 
                   ..., dbcon, dbtable = "msdata") {
    if (missing(dbcon) || !dbIsValid(dbcon)) {
        object@dbcon <- object@dbcon
    } else { 
        object@dbcon <- dbcon
    }
    if (is.data.frame(data))
        data <- DataFrame(data)
    if (!is(data, "DataFrame"))
        stop("'data' is supposed to be a 'DataFrame' with ",
             "spectrum data")
    pkey <- "_pkey"
    object@dbtable <- dbtable
    ## `data` can also be a `data.frame`
    if (nrow(data)) {
        data$dataStorage <- "<db>"
        .write_data_to_db(data, object@dbcon, dbtable = dbtable)
        ## Since we use `dbAppendTable` to write new data into SQLite db,
        ## The newly appended data will be at the end of the `msdata` table,
        ## The code below uses SQL statement to fetch the last `nrow(data)`
        ## _pkey as object@rows
        res <- dbGetQuery(object@dbcon, paste0("SELECT * FROM (SELECT ",
                          pkey, " FROM ", dbtable, " ORDER BY ", pkey, 
                          " DESC LIMIT ", nrow(data), ") ORDER BY ", pkey,
                          " ASC"))
        ## res is a data frame, we convert it into integer vector
        object@rows <- as.integer(res[, 1])
        } else {
    if (length(files)) {
        idx <- lapply(files, FUN = .write_mzR_to_db, con = object@dbcon,
                      dbtable = dbtable)
        ## We sum up all the numbers from idx list
        sum_idx <- Reduce("+", idx)
        ## Use the same way to fetch the last `sum_idx` _pkeys
        res <- dbGetQuery(object@dbcon, paste0("SELECT * FROM (SELECT ",
                          pkey, " FROM ", dbtable, " ORDER BY ", pkey, 
                          " DESC LIMIT ", sum_idx, ") ORDER BY ", pkey,
                          " ASC"))
        object@rows <- as.integer(res[, 1])
    } else {
        object@rows <- dbGetQuery(
            object@dbcon, paste0("select ", pkey, " from ", 
                                 dbtable))[, pkey]
    } }
    msg <- .valid_db_table_columns(object@dbcon, dbtable)
    if (length(msg)) stop(msg)
    cns <- colnames(dbGetQuery(object@dbcon, paste0("select * from ",
                                             dbtable, " limit 2")))
    object@columns <- cns[cns != pkey]
    object@query <- dbSendQuery(
        object@dbcon, paste0("select ? from ", dbtable, " where ",
                      pkey, "= ?"))
    object
})

#' `backendMerge` method for `MsBackendSqlDb` class. When a `dbcon` is provided,
#' a new `MsBackendSqlDb` instance will be created; the dbtable from `object` 
#' will be inserted into the newly created `MsBackendSqlDb` instance, using 
#' `ATTACH` SQL statement. Or if `dbcon` is missing, the dbtable from other 
#' `object` will be inserted into this `object` using the same schema migration
#' mechanism.
#' 
#' @param dbcon a `DBIConnection` object to connect to the database.
#' 
#' @importMethodsFrom Spectra backendMerge
#' 
#' @exportMethod backendMerge
#'
#' @rdname hidden_aliases
setMethod("backendMerge", "MsBackendSqlDb", function(object, ..., dbcon) {
    object <- unname(c(object, ...))
    ## If `MsBackendSqlDb` has no mz values, the list will not merge it
    object <- object[lengths(object) > 0]
    res <- suppressWarnings(.combine_backend_SqlDb(object, dbcon))
    res
})

## Data accessors
#' @importMethodsFrom Spectra acquisitionNum
#'
#' @rdname hidden_aliases
#' 
#' @importMethodsFrom ProtGenerics acquisitionNum
setMethod("acquisitionNum", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "acquisitionNum")
})

#' @rdname hidden_aliases
#' 
#' @importMethodsFrom ProtGenerics centroided
setMethod("centroided", "MsBackendSqlDb", function(object) {
    as.logical(.get_db_data(object, "centroided"))
})

#' @rdname hidden_aliases
#' 
#' @importMethodsFrom ProtGenerics collisionEnergy
setMethod("collisionEnergy", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "collisionEnergy")
})

#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics dataOrigin
setMethod("dataOrigin", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "dataOrigin")
})

#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics dataStorage
setMethod("dataStorage", "MsBackendSqlDb", function(object) {
    rep("<db>", length(object@rows))
})

#' @exportMethod intensity
#' 
#' @importMethodsFrom ProtGenerics intensity
#' 
#' @rdname hidden_aliases
setMethod("intensity", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "intensity")
})

#' @rdname hidden_aliases
#' 
#' @importFrom MsCoreUtils vapply1d
#'
#' @importMethodsFrom ProtGenerics ionCount
setMethod("ionCount", "MsBackendSqlDb", function(object) {
    vapply1d(intensity(object), sum, na.rm = TRUE)
})

#' @rdname hidden_aliases
#' @importFrom MsCoreUtils vapply1l
#'
#' @importMethodsFrom ProtGenerics isCentroided
setMethod("isCentroided", "MsBackendSqlDb", function(object, ...) {
    vapply1l(as.list(object), Spectra:::.peaks_is_centroided)
})

#' @rdname hidden_aliases
#'
#' @importMethodsFrom S4Vectors isEmpty
setMethod("isEmpty", "MsBackendSqlDb", function(x) {
    lengths(intensity(x)) == 0
})

#' @importMethodsFrom Spectra isolationWindowLowerMz
#' 
#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics isolationWindowLowerMz
setMethod("isolationWindowLowerMz", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "isolationWindowLowerMz")
})

#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics isolationWindowTargetMz
setMethod("isolationWindowTargetMz", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "isolationWindowTargetMz")
})

#' @importMethodsFrom Spectra isolationWindowUpperMz
#'
#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics isolationWindowUpperMz
setMethod("isolationWindowUpperMz", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "isolationWindowUpperMz")
})

#' @rdname hidden_aliases
setMethod("length", "MsBackendSqlDb", function(x) {
    length(x@rows)
})

#' @rdname hidden_aliases
setMethod("lengths", "MsBackendSqlDb", function(x, use.names = FALSE) {
    lengths(mz(x))
})

#' @importMethodsFrom Spectra msLevel
#'
#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics msLevel
setMethod("msLevel", "MsBackendSqlDb", function(object, ...) {
    .get_db_data(object, "msLevel")
})

#' @exportMethod mz
#' 
#' @importMethodsFrom ProtGenerics mz
#' 
#' @rdname hidden_aliases
setMethod("mz", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "mz")
})

#' @importMethodsFrom BiocGenerics as.list
#' 
#' @exportMethod as.list
#'
#' @rdname hidden_aliases
setMethod("as.list", "MsBackendSqlDb", function(x) {
    mapply(cbind, mz = mz(x), intensity = intensity(x),
           SIMPLIFY = FALSE, USE.NAMES = FALSE)
})


#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics polarity
setMethod("polarity", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "polarity")
})

#' @importMethodsFrom Spectra precScanNum
#'
#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics precScanNum
setMethod("precScanNum", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "precScanNum")
})

#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics precursorCharge
setMethod("precursorCharge", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "precursorCharge")
})

#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics precursorIntensity
setMethod("precursorIntensity", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "precursorIntensity")
})

#' @importMethodsFrom Spectra precursorMz
#'
#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics precursorMz
setMethod("precursorMz", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "precursorMz")
})

#' @importMethodsFrom Spectra reset
#'
#' @rdname hidden_aliases
setMethod("reset", "MsBackendSqlDb", function(object) {
    .reset_row_indices(object)
})

#' @importMethodsFrom Spectra rtime
#'
#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics rtime
setMethod("rtime", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "rtime")
})

#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics scanIndex
setMethod("scanIndex", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "scanIndex")
})

#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics smoothed
setMethod("smoothed", "MsBackendSqlDb", function(object) {
    results <- .get_db_data(object, "smoothed")
    if (identical(results, "Columns missing from database.")) {
        return("Columns missing from database.")
    } else {
        as.logical(results)
    }
})

#' @rdname hidden_aliases
#'
#' @importFrom methods as
#'
#' @importFrom S4Vectors SimpleList
#' 
#' @importMethodsFrom Spectra spectraData
#' 
#' @importMethodsFrom DBI dbListFields
#' 
#' @exportMethod spectraData
setMethod("spectraData", "MsBackendSqlDb",
          function(object, columns = spectraVariables(object)) {
    res <- .get_db_data(object, columns)
    ## According to the limitation of SQLite, some of the column types
    ## are missing from the returning values, such as boolean/logical values
    ## We have to change these column types to the correct ones.
    colsToChange <- names(res)[names(res) %in% 
                                 names(Spectra:::.SPECTRA_DATA_COLUMNS)]
    colsToChange <- colsToChange[!colsToChange %in% c("mz", "intensity")]
    for(i in colsToChange){
        class(res[, i]) <- Spectra:::.SPECTRA_DATA_COLUMNS[i]
        if (Spectra:::.SPECTRA_DATA_COLUMNS[i] == "logical")
            res[, i] <- ifelse(res[, i] == 0, FALSE, TRUE)
    }
    res
})

#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics spectraNames
setMethod("spectraNames", "MsBackendSqlDb",
          function(object) NULL)

#' @importMethodsFrom ProtGenerics spectraVariables
#' 
#' @importMethodsFrom DBI dbListFields
#' 
#' @exportMethod spectraVariables
#' 
#' @rdname hidden_aliases
setMethod("spectraVariables", "MsBackendSqlDb", function(object) {
    dbfields <- dbListFields(object@dbcon, object@dbtable)
    dbfields <- dbfields[!(dbfields %in% "_pkey")]
    unique(c(names(Spectra:::.SPECTRA_DATA_COLUMNS), dbfields))
})

#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics tic
setMethod("tic", "MsBackendSqlDb", function(object, initial = TRUE) {
    if (initial) {
      if (any(dbListFields(object@dbcon, object@dbtable) == "totIonCurrent"))
        .get_db_data(object, "totIonCurrent")
      else rep(NA_real_, times = length(object))
    }   else vapply1d(intensity(object), sum, na.rm = TRUE)
})

#' @rdname hidden_aliases
setMethod("$", signature = "MsBackendSqlDb", 
          function(x, name) {
              res <- .get_db_data(x, name[1])
              res 
})


#' @rdname hidden_aliases
#' 
setReplaceMethod("$", "MsBackendSqlDb", function(x, name, value) {
    if (!(length(value) == length(x)))
        stop("Provided values has different length than the object.") 
    if (name %in% spectraVariables(x)) {
        x <- .update_db_table_columns(x, name, value)
    } else {
        x <- .insert_db_table_columns(x, name, value)
    }
    x
})

#### ---------------------------------------------------------------------------
##
##                      FILTERING AND SUBSETTING
##
#### ---------------------------------------------------------------------------

#' @importMethodsFrom S4Vectors [
#'
#' @importFrom MsCoreUtils i2index
#'
#' @rdname hidden_aliases
setMethod("[", "MsBackendSqlDb", function(x, i, j, ..., drop = FALSE) {
    .subset_backend_SqlDb(x, i)
})

#' @rdname hidden_aliases
setMethod("split", "MsBackendSqlDb", function(x, f, drop = FALSE, ...) {
    if (!is.factor(f))
        f <- as.factor(f)
    idx <- split(seq_along(x@rows), f, ...)
    output <- vector(length = length(idx), mode = "list")
    for (i in seq_along(idx)) {
        slot(x, "rows", check = FALSE) <- idx[[i]]
        output[[i]] <- x
    }
    output
})

#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics filterAcquisitionNum
setMethod("filterAcquisitionNum", "MsBackendSqlDb",
          function(object, n = integer(), dataStorage = character(),
                   dataOrigin = character()) {
    if (!length(n) || !length(object)) return(object)
    if (!is.integer(n)) stop("'n' has to be an integer representing the ",
                                     "acquisition number(s) for sub-setting")
    sel_file <- .sel_file_sql(object, dataStorage, dataOrigin)
    sel_acq <- acquisitionNum(object) %in% n & sel_file
    object@rows <- object@rows[sel_acq | !sel_file]
    object        
})

#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics filterDataOrigin
setMethod("filterDataOrigin", "MsBackendSqlDb",
          function(object, dataOrigin = character()) {
    if (length(dataOrigin)) {
        object@rows <- object@rows[dataOrigin(object) %in% dataOrigin]
        if (is.unsorted(dataOrigin))
            object@rows <- object@rows[order(match(dataOrigin(object), 
                                                   dataOrigin))]
        else object
    } else object
})

#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics filterDataStorage
setMethod("filterDataStorage", "MsBackendSqlDb",
          function(object, dataStorage = character()) {
    if (length(dataStorage)) {
        object@rows <- object@rows[dataStorage(object) %in% dataStorage]
        if (is.unsorted(dataStorage))
            object@rows <- object@rows[order(match(dataStorage(object), 
                                                   dataStorage))]
        else object
    } else object
})

#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics filterEmptySpectra
setMethod("filterEmptySpectra", "MsBackendSqlDb", function(object) {
    if (!length(object)) return(object)
    object@rows <- object@rows[as.logical(lengths(object))]
    object
})

#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics filterIsolationWindow
setMethod("filterIsolationWindow", "MsBackendSqlDb", {
  function(object, mz = numeric(), ...) {
    if (length(mz)) {
        if (length(mz) > 1)
            stop("'mz' is expected to be a single m/z value", call. = FALSE)
        keep <- which(isolationWindowLowerMz(object) <= mz &
                      isolationWindowUpperMz(object) >= mz)
        object@rows <- object@rows[keep]
        object
    } else object
  }
})

#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics filterMsLevel
setMethod("filterMsLevel", "MsBackendSqlDb",
          function(object, msLevel = integer()) {
    if (length(msLevel)) {
        object@rows <- object@rows[msLevel(object) %in% msLevel]
        object
    } else object
})

#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics filterPolarity
setMethod("filterPolarity", "MsBackendSqlDb",
          function(object, polarity = integer()) {
    if (length(polarity)) {
        object@rows <- object@rows[polarity(object) %in% polarity]
        object
    } else object
})

#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics filterPrecursorMz
setMethod("filterPrecursorMz", "MsBackendSqlDb",
          function(object, mz = numeric()) {
    if (length(mz)) {
        mz <- range(mz)
        keep <- which(precursorMz(object) >= mz[1] &
                      precursorMz(object) <= mz[2])
        object@rows <- object@rows[keep]
        object
    } else object
})



#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics filterPrecursorScan
setMethod("filterPrecursorScan", "MsBackendSqlDb",
          function(object, acquisitionNum = integer()) {
    if (length(acquisitionNum)) {
        object@rows <- object@rows[Spectra:::.filterSpectraHierarchy(
                                       acquisitionNum(object),
                                       precScanNum(object),
                                       acquisitionNum)]
        object
    } else object
})

#' @rdname hidden_aliases
#'
#' @importMethodsFrom ProtGenerics filterRt
setMethod("filterRt", "MsBackendSqlDb",
          function(object, rt = numeric(), 
                   msLevel. = unique(msLevel(object))) {
    if (length(rt)) {
        rt <- range(rt)
        sel_ms <- msLevel(object) %in% msLevel.
        sel_rt <- rtime(object) >= rt[1] & rtime(object) <= rt[2] & sel_ms
        object@rows <- object@rows[sel_rt | !sel_ms]
        object
    } else object
})
