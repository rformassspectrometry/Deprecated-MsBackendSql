#' @include hidden_aliases.R
NULL

#' @rdname MsBackendSqlDb
#'
#' @importFrom methods is new
#'
#' @export
MsBackendSqlDb <- function() {
    new("MsBackendSqlDb")
}

#' Test if db table is available
#'
#' @param dbcon [`DBIConnection-class`] object
#' @param dbtable `character(1)`, table name
#' @author Johannes Rainer, Sebastian Gibb
#' @noRd
.valid_db_table_exists <- function(dbcon, dbtable) {
    if (!dbExistsTable(dbcon, dbtable))
        paste0("database table '", dbtable, "' not found")
    else
        NULL
}

#' Test for required columns in db table
#'
#' Checks whether all required columns are present and of the correct data type.
#'
#' @param dbcon [`DBIConnection-class`] object
#' @param dbtable `character(1)`, table name
#' @param columns `character`, user defined columns that have to be present (no
#' type check available)
#' @param pkey `character(1)`, name of the PRIMARY KEY column
#'
#' @author Johannes Rainer, Sebastian Gibb
#' @noRd
.valid_db_table_columns <- function(dbcon, dbtable,
                                    columns = character(), pkey = "_pkey") {
    req_cols <- c("integer", # pkey
                  dataStorage = "character",
                  dataOrigin = "character",
                  intensity = "blob",
                  msLevel = "integer",
                  mz = "blob",
                  rtime = "numeric")
    names(req_cols)[1L] <- pkey
    cn <- unique(c(names(req_cols), columns))

    r <- dbGetQuery(dbcon, paste0("SELECT * FROM ", dbtable, " LIMIT 0"))

    if (!all(cn %in% names(r)))
        return(paste0("required column(s) ",
                      paste0(cn[!cn %in% names(r)], collapse = ","),
                      " not found"))

    isCorrectType <- mapply(is, object = r[names(req_cols)], class2 = req_cols)
    if (!all(isCorrectType))
        return(paste0("required column(s) ",
                      paste0(names(req_cols)[!isCorrectType],
                             collapse = ","), " has/have the wrong data type"))
    NULL
}

#' Read the data from a single mzML file and store it to the database.
#'
#' @importMethodsFrom S4Vectors as.data.frame
#'
#' @author Johannes Rainer
#'
#' @noRd
.write_mzR_to_db <- function(x = character(), con = NULL, dbtable = "msdata") {
    hdr <- Spectra:::.mzR_header(x)
    hdr <- as.data.frame(hdr)
    pks <- Spectra:::.mzR_peaks(x, hdr$scanIndex)
    hdr$mz <- lapply(pks, "[", , 1)
    hdr$intensity <- lapply(pks, "[", , 2)
    hdr$dataOrigin <- x
    hdr$dataStorage <- "<db>"
    rm(pks)
    missingCol <- setdiff(names(Spectra:::.SPECTRA_DATA_COLUMNS), names(hdr))
    if (length(missingCol) > 0)
        hdr[, setdiff(names(Spectra:::.SPECTRA_DATA_COLUMNS), names(hdr))] <- NA 
    .write_data_to_db(hdr, con = con, dbtable = dbtable)
}

#' @importFrom DBI dbExecute dbAppendTable dbExistsTable dbDataType
#'
#' @importFrom MsCoreUtils vapply1l
#'
#' @noRd
.write_data_to_db <- function(x, con, dbtable = "msdata") {
    basic_type <- c("integer", "numeric", "logical", "factor", "character")
    is_blob <- which(!vapply1l(x, inherits, basic_type))
    for (i in is_blob) {
        x[[i]] <- lapply(x[[i]], base::serialize, NULL)
    }
    if (!dbExistsTable(con, dbtable)) {
        flds <- dbDataType(con, x)
        if (inherits(con, "SQLiteConnection"))
            flds <- c(flds, `_pkey` = "INTEGER PRIMARY KEY")
        else stop(class(con)[1], " connections are not yet supported.")
        ## mysql INT AUTO_INCREMENT
        qr <- paste0("create table '", dbtable, "' (",
                     paste(paste0("'", names(flds), "'"), flds,
                           collapse = ", "), ")")
        res <- dbExecute(conn = con, qr)
    }
    dbAppendTable(conn = con, name = dbtable, x)
}

#' Get data from the database and ensure the right data type is returned.
#'
#' @importFrom DBI dbSendQuery dbBind dbFetch dbClearResult
#'
#' @importFrom MsCoreUtils vapply1l
#'
#' @importFrom IRanges NumericList
#'
#' @importFrom S4Vectors DataFrame
#'
#' @noRd
.get_db_data <- function(object, columns = character()) {
    if (length(setdiff(columns, object@columns)) == 0) {
        qry <- dbSendQuery(object@dbcon,
                           paste0("select ", paste(columns, collapse = ","),
                                  " from ", object@dbtable, " where _pkey = ?"))
        qry <- dbBind(qry, list(object@rows))
        res <- dbFetch(qry)
        dbClearResult(qry)
        is_blob <- which(vapply1l(res, is, "blob"))
        for (i in is_blob)
            res[[i]] <- lapply(res[[i]], unserialize)
        if (ncol(res) == 1) {
            if (any(c("mz", "intensity") %in% colnames(res)))
                return(NumericList(res[[1]], compress = FALSE))
            else return(res[[1]])
        }
        res <- DataFrame(res)
        mzint <- which(colnames(res) %in% c("mz", "intensity"))
        for (i in mzint)
            res[[i]] <- NumericList(res[[i]])
        res 
    } else {
        return("Columns missing from database.")
    }
}

#' @param x a `MsBackendSqlDb` object.
#' 
#' @noRd
.getDbTable <- function(x) {
    x@dbtable
}

#' @param x a `MsBackendSqlDb` object.
#' 
#' @noRd
.getModCount <- function(x) {
    x@modCount
}

#' @param x a `MsBackendSqlDb` object.
#' 
#' @noRd
.getRows <- function(x) {
    x@rows
}

#' @param x a `MsBackendSqlDb` object.
#' 
#' @noRd
.getColumns <- function(x) {
    x@columns
}

#' @param x a `MsBackendSqlDb` object.
#' 
#' @noRd
.getQuery <- function(x) {
    x@query
}


#' Replace the columns from the database and ensure the right data type can be 
#'   returned. 
#'
#' @importFrom DBI dbSendQuery dbExecute dbClearResult dbReadTable dbWriteTable
#'
#' @noRd
.replace_db_table_columns <- function(object, column, value) {
    str1 <- object@columns[object@columns != column]
    sql1 <- paste0("CREATE VIEW metaview AS SELECT ",
                   toString(str1), " FROM ", object@dbtable)
    qry <- dbSendQuery(object@dbcon, sql1)
    dbClearResult(qry)
    sql2 <- paste0("CREATE VIEW metakey AS SELECT ", "_pkey",
                   " FROM ", object@dbtable)
    qry2 <- dbSendQuery(object@dbcon, sql2)
    dbClearResult(qry2)
    metapkey <- dbReadTable(object@dbcon, 'metakey')
    dbWriteTable(object@dbcon, 'token', 
                 data.frame(value, pkey = metapkey))
    sql3 <- paste0("CREATE TABLE msdata1 AS ", "SELECT * FROM metaview ",
                   "INNER JOIN token on token._pkey = metaview1._pkey")
    dbExecute(object@dbcon, sql3)
    dbExecute(object@dbcon, paste0("ALTER TABLE ", object@dbtable,
                                    " RENAME TO _msdata_old"))
    dbExecute(object@dbcon, "ALTER TABLE msdata1 RENAME TO ", 
              object@dbtable)
    dbExecute(object@dbcon, "DROP TABLE IF EXISTS _msdata_old")
    ## Drop View
    dbExecute(object@dbcon, "DROP TABLE IF EXISTS token")
    dbExecute(object@dbcon, "DROP VIEW IF EXISTS metaview")
}