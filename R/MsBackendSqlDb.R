#' @include hidden_aliases.R
NULL

#' @title SQL database based mass spectrometry data backend
#'
#' @aliases class:MsBackendSqlDb MsBackendSqlDb-class
#'
#' @description
#'
#' The `MsBackendSqlDb` is an [MsBackend] backend which keeps all spectra data
#' (metadata as well as m/z and intensity values) in a single database table.
#' This object has a very low memory footprint because only primary keys of the
#' database table are stored within the object hence making it ideal for
#' representing very large mass spectrometry data sets on computers with limited
#' resources.
#'
#' @param object a `MsBackendSqlDb` object.
#'
#' @param dbcon a `DBIConnection` object to connect to the database.
#'
#' @param files `character` with the file names from which data should be
#'     imported. Only required if the database to which `dbcon` connects does
#'     not already contain the data.
#'
#' @param dbtable `character(1)` the name of the database table with the data.
#'     Defaults to `dbtable = "msdata"`.
#'
#' @section Creating an `MsBackendSqlDb` instance:
#'
#' Like all [MsBackend()] objects, `MsBackendSqlDb` have to be *initialized*
#' with the `backendInitialize` method that either fills the object with data
#' from mass spectrometry files provided with the `files` parameter or checks
#' if the pre-generated database contains all required data. A connection to
#' a database (with write access) has to be provided with the `dbcon` parameter
#' which expects a `DBIConnection` returned by a call to [dbConnect()]. To
#' fill the database with MS data files (usually mzML files, but all files
#' types supported by the `mzR` package are allowed), these have to be provided
#' with the `files` parameter. Parameter `dbtable` can be used to optionally
#' specify the database table name that contains, or should contain the data.
#'
#' @section Implementation notes:
#'
#' The `MsBackendSqlDb` defines the following slots which should not be
#' accessed or changed by the user, which can be retrieved by `getter` 
#' functions:
#'
#' - `@dbtable` (`character`): the name of the database table (or view)
#'   containing the data.
#' - `@dbcon` (`DBIConnection`): the connection to the database.
#' - `@modCount` (`integer`): keeps track of each database writing cycle which
#'   would invalidate objects pointing to the same database. This number has
#'   to match between the database and the object.
#' - `@rows` (`integer`): the indices (primary keys) of the data.
#' - `@columns` (`character`): the names of the columns stored in the database.
#'
#' @name MsBackendSqlDb
#'
#' @author Johannes Rainer, Chong Tang.
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
    msg <- .valid_db_table_columns(object@dbcon, object@dbtable)
    msg <- c(msg, .valid_db_table_has_columns(object@dbcon, object@columns))
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

## Data accessors

#' @rdname hidden_aliases
setMethod("acquisitionNum", "MsBackendSqlDb", function(object) {
  .get_db_data(object, "acquisitionNum")
})

#' @rdname hidden_aliases
setMethod("centroided", "MsBackendSqlDb", function(object) {
    as.logical(.get_db_data(object, "centroided"))
})

#' @rdname hidden_aliases
setMethod("collisionEnergy", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "collisionEnergy")
})

#' @rdname hidden_aliases
setMethod("dataOrigin", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "dataOrigin")
})

#' @rdname hidden_aliases
setMethod("dataStorage", "MsBackendSqlDb", function(object) {
    rep("<db>", length(object))
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
setMethod("ionCount", "MsBackendSqlDb", function(object) {
    vapply1d(intensity(object), sum, na.rm = TRUE)
})

#' @rdname hidden_aliases
#' @importFrom MsCoreUtils vapply1l
setMethod("isCentroided", "MsBackendSqlDb", function(object, ...) {
    vapply1l(as.list(object), Spectra:::.peaks_is_centroided)
})

#' @rdname hidden_aliases
setMethod("isEmpty", "MsBackendSqlDb", function(x) {
    lengths(intensity(x)) == 0
})

#' @rdname hidden_aliases
setMethod("isolationWindowLowerMz", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "isolationWindowLowerMz")
})

#' @rdname hidden_aliases
setMethod("isolationWindowTargetMz", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "isolationWindowTargetMz")
})

#' @rdname hidden_aliases
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

#' @rdname hidden_aliases
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
setMethod("polarity", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "polarity")
})

#' @rdname hidden_aliases
setMethod("precScanNum", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "precScanNum")
})

#' @rdname hidden_aliases
setMethod("precursorCharge", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "precursorCharge")
})

#' @rdname hidden_aliases
setMethod("precursorIntensity", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "precursorIntensity")
})

#' @rdname hidden_aliases
setMethod("precursorMz", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "precursorMz")
})

#' @rdname hidden_aliases
setMethod("rtime", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "rtime")
})

#' @rdname hidden_aliases
setMethod("scanIndex", "MsBackendSqlDb", function(object) {
    .get_db_data(object, "scanIndex")
})

#' @rdname hidden_aliases
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
    dbfields <- dbListFields(object@dbcon, object@dbtable)
    dbfields <- dbfields[!(dbfields %in% "_pkey")]
    df_columns <- intersect(columns, dbfields)
    res <- .get_db_data(object, columns)
    other_columns <- setdiff(columns, dbfields)
    if (length(other_columns)) {
        other_res <- .get_db_data(object, other_columns)
        names(other_res) <- other_columns
        is_mz_int <- names(other_res) %in% c("mz", "intensity")
        if (!all(is_mz_int))
            res <- cbind(res, as(other_res[!is_mz_int], "DataFrame"))
        if (any(names(other_res) == "mz"))
            res$mz <- if (length(other_res$mz)) other_res$mz
        else NumericList(compress = FALSE)
        if (any(names(other_res) == "intensity"))
            res$intensity <- if (length(other_res$intensity)) other_res$intensity
        else NumericList(compress = FALSE)
      }
    res[, columns, drop = FALSE]
})

#' @rdname hidden_aliases
setMethod("spectraNames", "MsBackendSqlDb", function(object) {
    rep(NA_real_, times = length(object))
})

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
setMethod("tic", "MsBackendSqlDb", function(object, initial = TRUE) {
    if (initial) {
      if (any(dbListFields(object@dbcon, object@dbtable) == "totIonCurrent"))
        .get_db_data(object, "totIonCurrent")
      else rep(NA_real_, times = length(object))
    }   else vapply1d(intensity(object), sum, na.rm = TRUE)
})

#' @rdname hidden_aliases
#' 
setMethod("$", signature = "MsBackendSqlDb", 
          function(x, name) {
    if (!is.null(.valid_db_table_has_columns(x@dbcon, 
                                             x@dbtable, 
                                             name)))
    stop("spectra variable '", name, "' not available")
    if (length(name) == 1) {
        res <- .get_db_data(x, name)
        res }
    else {
        res <- .get_db_data(x, name[1])
        res }
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