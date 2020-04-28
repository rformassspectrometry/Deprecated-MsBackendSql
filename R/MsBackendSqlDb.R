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
                               rows = integer(),
                               columns = character(),
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

#' @rdname hidden_aliases
#' 
setMethod("sqlVariables", "MsBackendSqlDb", function(object) {
  unique(names(.SPECTRA_DATA_COLUMNS))
})


#' @rdname hidden_aliases
#' 
setMethod("$", signature = "MsBackendSqlDb", 
          function(x, columns = x@columns) {
    if (!is.null(.valid_db_table_has_columns(x@dbcon, 
                                             x@dbtable, 
                                             columns)))
    stop("spectra variable '", columns, "' not available")
    .get_db_data(x, columns)[, 1]
})

#' @rdname hidden_aliases
#' 
setReplaceMethod("$", "MsBackendSqlDb", function(x, columns, value) {
  basic_type <- c("integer", "numeric", "logical", "factor", "character")
  if (is.list(value) && any(c("mz", "intensity") == columns) && 
      !inherits(value, basic_type))
    value <- lapply(value, base::serialize, NULL)
  x@spectraData[[columns]] <- value
  validObject(x)
  x
})