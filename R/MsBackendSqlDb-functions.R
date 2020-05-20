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

#' @description
#'
#' Check if SQLite table has all required columns.
#' 
#' @param dbcon a `DBIConnection` object to connect to the database.
#' 
#' @param dbtable `character(1)` the name of the database table with the data.
#'     Defaults to `dbtable = "msdata"`.
#'
#' @noRd
.valid_db_table_columns <- function(dbcon, dbtable, pkey = "pkey") {
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

#' data types of spectraData columns
#'
#' @noRd
.SPECTRA_DATA_COLUMNS <- c(
    msLevel = "integer",
    rtime = "numeric",
    acquisitionNum = "integer",
    scanIndex = "integer",
    mz = "NumericList",
    intensity = "NumericList",
    dataStorage = "character",
    dataOrigin = "character",
    centroided = "logical",
    smoothed = "logical",
    polarity = "integer",
    precScanNum = "integer",
    precursorMz = "numeric",
    precursorIntensity = "numeric",
    precursorCharge = "integer",
    collisionEnergy = "numeric",
    isolationWindowLowerMz = "numeric",
    isolationWindowTargetMz = "numeric",
    isolationWindowUpperMz = "numeric"
)

#' @description
#'
#' Check if SQLite table has the specified columns by the users.
#' 
#' @param dbcon a `DBIConnection` object to connect to the database.
#' 
#' @param dbtable `character(1)` the name of the database table with the data.
#'     Defaults to `dbtable = "msdata"`.
#'     
#' @param columns `character` the names of the columns specified by the users.
#'
#' @noRd
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
    hdr <- Spectra:::.mzR_header(x)
    hdr <- as.data.frame(hdr)
    pks <- Spectra:::.mzR_peaks(x, hdr$scanIndex)
    hdr$mz <- lapply(pks, "[", , 1)
    hdr$intensity <- lapply(pks, "[", , 2)
    hdr$dataOrigin <- x
    hdr$dataStorage <- "<db>"
    rm(pks)
    msg <- is.null(setdiff(names(.SPECTRA_DATA_COLUMNS), names(hdr)))
    if (!is.null(msg))
        nr_x <- nrow(hdr)
        if (nr_x)
            set_diff <- setdiff(names(.SPECTRA_DATA_COLUMNS), names(hdr))
            ncol1 <- length(set_diff)
            df1 <- data.frame(matrix(ncol = ncol1, nrow = nr_x))
            colnames(df1) <- set_diff
    hdr <- cbind(hdr, df1)
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
            flds <- c(flds, `pkey` = "INTEGER PRIMARY KEY")
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
.get_db_data <- function(object, columns) {
    qry <- dbSendQuery(object@dbcon,
                       paste0("select ", paste(columns, collapse = ","),
                              " from ", object@dbtable, " where pkey = ?"))
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
    sql2 <- paste0("CREATE VIEW metakey AS SELECT ", "pkey",
                   " FROM ", object@dbtable)
    qry2 <- dbSendQuery(object@dbcon, sql2)
    dbClearResult(qry2)
    metapkey <- dbReadTable(object@dbcon, 'metakey')
    dbWriteTable(object@dbcon, 'token', 
                 data.frame(value, pkey = metapkey))
    sql3 <- paste0("CREATE TABLE msdata1 AS ", "SELECT * FROM metaview ",
                   "INNER JOIN token on token.pkey = metaview1.pkey")
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