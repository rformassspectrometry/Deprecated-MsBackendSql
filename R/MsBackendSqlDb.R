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
#' @importFrom utils capture.output
#' 
#' @rdname hidden_aliases
setMethod("show", "MsBackendSqlDb", function(object) {
    spd <- asDataFrame(object, c("msLevel", "rtime", "scanIndex"))
    cat(class(object), "with", nrow(spd), "spectra\n")
    if (nrow(spd)) {
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
          function(object, dbcon, files = character(), dbtable = "msdata") {
    if (missing(dbcon) || !dbIsValid(dbcon))
        stop("A valid connection to a database has to be provided",
             " with parameter 'dbcon'. See ?MsBackendSqlDb for more",
             " information.")
    pkey <- "_pkey"
    object@dbcon <- dbcon
    object@dbtable <- dbtable
    if (length(files)) {
        idx <- lapply(files, FUN = .write_mzR_to_db, con = dbcon,
                      dbtable = dbtable)
        object@rows <- seq_len(sum(unlist(idx, use.names = FALSE)))
    } else {
        object@rows <- dbGetQuery(
            dbcon, paste0("select ", pkey, " from ", dbtable))[, pkey]
    }
    msg <- .valid_db_table_columns(dbcon, dbtable)
    if (length(msg)) stop(msg)
    cns <- colnames(dbGetQuery(dbcon, paste0("select * from ",
                                             dbtable, " limit 2")))
    object@columns <- cns[cns != pkey]
    object@query <- dbSendQuery(
        dbcon, paste0("select ? from ", dbtable, " where ",
                      pkey, "= ?"))
    object
})

#' 
#'
#' @param processingQueue For `Spectra`: optional `list` of
#'     [ProcessingStep-class] objects.
#'     
#' @param metadata For `Spectra`: optional `list` with metadata information.
#' 
#' @param backend For `Spectra`: [MsBackend-class] to be used as backend. 
#'
#' @importClassesFrom Spectra Spectra
#'
#' @rdname MsBackendSqlDb
setMethod("Spectra", "MsBackendSqlDb", function(object, processingQueue = list(),
                                           metadata = list(), ...,
                                           dbcon = dbcon,
                                           backend = MsBackendSqlDb()) {
    new("Spectra", metadata = metadata, processingQueue = processingQueue,
         backend = backendInitialize(backend, ...))
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
    as.logical(.get_db_data(object, "smoothed"))
})

#' @rdname hidden_aliases
#'
#' @importFrom methods as
#'
#' @importFrom S4Vectors SimpleList
#' 
#' @importMethodsFrom Spectra asDataFrame
#' 
#' @importMethodsFrom DBI dbListFields
#' 
#' @exportMethod asDataFrame
setMethod("asDataFrame", "MsBackendSqlDb",
          function(object, columns = spectraVariables(object)) {
             .get_db_data(object, columns)
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
  if (!(length(list(x@rows)) == length(value)))
  stop("Provided values has different length than the column.")
  basic_type <- c("integer", "numeric", "logical", "factor", "character")
  if (is.list(value) && any(c("mz", "intensity") == name) && 
       inherits(value, basic_type))
    value <- lapply(value, base::serialize, NULL)
  .replace_db_table_columns(x, name, value)
  x@modCount <- x@modCount + 1L
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
    return(output)
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



#' Filter the intensity values from a `MsBackendSqlDb` object.  If
#' only one numeric parameter is provided, the returned intensity will
#' keep any values larger than the parameter. If two values are
#' provided, only the intensity values between the two parameters will
#' be preserved.
#'
#' @param x a `MsBackendSqlDb` object.
#' 
#' @param intensities a `numeric` vector either be length 1 or 2.
#' 
#' @importFrom IRanges NumericList
#' 
#' @export
#' 
#' @rdname hidden_aliases
filterIntensity <- function(x, intensities = numeric()) {
    if (length(intensities) == 1) {
        filteredIntensity <- lapply(intensity(x), 
                                    function(i) (i[i > intensities]))
        filteredIntensity <- NumericList(filteredIntensity, compress = FALSE)
    } else if (length(intensities) == 2) {
        filteredIntensity <- lapply(intensity(x), 
                                    function(i) (i[i > intensities[1] &
                                                   i < intensities[2]]))
        filteredIntensity <- NumericList(filteredIntensity, compress = FALSE)
    } else {
        stop("intensities must be of length 1 or 2. See man page for details.")
    }
}
