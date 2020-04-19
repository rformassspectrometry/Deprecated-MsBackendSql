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

.valid_db_table_columns <- function(dbcon, dbtable, pkey = "_pkey") {
    if (!dbExistsTable(dbcon, dbtable))
        return(paste0("database table '", dbtable, "' not found"))
    tmp <- dbGetQuery(dbcon, paste0("select * from ", dbtable, " limit 3"))
    if (!any(colnames(tmp) == pkey))
        return(paste0("required column '", pkey ,"' not found"))
    req_cols <- c(dataStorage = "character",
                  dataOrigin = "character",
                  rtime = "numeric",
                  msLevel = "integer")
    if (!all(names(req_cols) %in% colnames(tmp)))
        return(
            paste0("required column(s) ",
                   paste0(names(req_cols)[!names(req_cols) %in% colnames(tmp)],
                          collapse = ", "), " not found"))
    classes <- vapply(tmp[, names(req_cols)], class, character(1))
    if (!all(classes == req_cols))
        return(
            paste0("required column(s) ",
                   paste0(names(req_cols)[classes != req_cols], collapse = ", "),
                   " have the wrong data type"))
    if (!any(colnames(tmp) == "mz") || !any(colnames(tmp) == "intensity"))
        return("required columns 'mz' and 'intensity' not found")
    if (!is(tmp$mz, "blob") || !is(tmp$intensity, "blob"))
        return("required columns 'mz' and 'intensity' have the wrong data type")
    NULL
}

.valid_db_table_has_columns <- function(dbcon, dbtable, columns) {
    tmp <- dbGetQuery(dbcon, paste0("select * from ", dbtable, " limit 3"))
    if (!all(columns %in% colnames(tmp)))
        return(paste0("columns ",
                      paste(columns[!columns %in% colnames(tmp)],
                            collapse = ", "), " not found in ", dbtable))
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
    hdr <- as.data.frame(Spectra:::.mzR_header(x))
    pks <- Spectra:::.mzR_peaks(x, hdr$scanIndex)
    hdr$mz <- lapply(pks, "[", , 1)
    hdr$intensity <- lapply(pks, "[", , 2)
    hdr$dataOrigin <- x
    hdr$dataStorage <- "<db>"
    rm(pks)
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
.get_db_data <- function(object, columns = object@columns) {
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
}
