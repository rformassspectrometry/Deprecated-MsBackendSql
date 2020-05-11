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
#' @param spectraColumn "DataFrame" Internal DataFrame to caching some columns
#'     for modification.
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
#' accessed or changed by the user:
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
                   query = "DBIResult",
                   spectraColumn = "DataFrame"),
         prototype = prototype(spectraData = "msdata",
                               readonly = FALSE,
                               modCount = 0L,
                               rows = integer(0),
                               columns = character(0),
                               spectraColumn = DataFrame(),
                               version = "0.1"))

setValidity("MsBackendSqlDb", function(object) {
  msg <- .valid_db_table_columns(object@dbcon, object@dbtable)
  msg <- c(msg, .valid_db_table_has_columns(object@dbcon, object@columns))
  if (is.null(msg)) TRUE
  else msg
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
            pkey <- "pkey"
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

#' @rdname hidden_aliases
setMethod("intensity", "MsBackendSqlDb", function(object) {
  msg <- .valid_db_table_has_columns(object@dbcon, 
                                     object@dbtable, 
                                     "intensity")
  if (is.null(msg)) 
    .get_db_data(object, "intensity")
  else {
    lst <- NumericList(numeric(), compress = FALSE)
    lst[rep(1, times = length(object))]
  }
})

#' @rdname hidden_aliases
#' @importFrom MsCoreUtils vapply1d
setMethod("ionCount", "MsBackendSqlDb", function(object) {
  vapply1d(intensity(object), sum, na.rm = TRUE)
})

#' @rdname hidden_aliases
setMethod("length", "MsBackendSqlDb", function(x) {
  length(x@rows)
})

#' @rdname hidden_aliases
setMethod("spectraColumn", "MsBackendSqlDb", function(object, columns) {
    msg <- .valid_db_table_has_columns(object@dbcon, 
                                       object@dbtable,
                                       columns)
    if (is.null(msg))
        object@spectraColumn <- .get_db_data(object, columns)
})


#' @rdname hidden_aliases
#' 
setMethod("$", signature = "MsBackendSqlDb", 
          function(x, name) {
    if (!is.null(.valid_db_table_has_columns(x@dbcon, 
                                             x@dbtable, 
                                             name)))
    stop("spectra variable '", name, "' not available")
    res <- .get_db_data(x, name)[, 1]
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
  validObject(x)
  x
})