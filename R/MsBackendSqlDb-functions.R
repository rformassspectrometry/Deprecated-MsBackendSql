#' @include hidden_aliases.R
NULL

#' @param dbcon a `DBIConnection` object to connect to the database.
#'
#' @rdname MsBackendSqlDb
#'
#' @importFrom methods is new
#'
#' @export MsBackendSqlDb
MsBackendSqlDb <- function(dbcon) {
    if (!requireNamespace("DBI", quietly = TRUE))
        stop("The use of 'MsBackendSqlDb' requires package 'DBI'. Please ",
             "install with 'install.packages(\"DBI\")'")
    if (missing(dbcon)) {
        x <- new("MsBackendSqlDb")
        slot(x, "dbcon", check = FALSE) <- dbConnect(SQLite(), tempfile(fileext = ".db"))
        return(x)
    } else {
      x <- new("MsBackendSqlDb")
      slot(x, "dbcon", check = FALSE) <- dbcon
      return(x)
    }
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

#' Helper function: will initiate a SQLite table `dbtable` in the SQLite 
#' database with defined column types. SQLite database uses a dynamic type 
#' system, which means if we simply copy a SQLite table into another SQLite 
#' database, the column types will be lost. So we have to create the column 
#' types for the copied table.
#' 
#' @param x data used to initiate a SQLite table in the database,
#'  `data.frame` format.
#' 
#' @param con [SQLiteConnection] object linking to the SQLite database file.
#' 
#' @param dbtale character vector containing SQLite table name.
#'
#' @importFrom DBI dbExecute dbExistsTable dbDataType
#'
#' @importFrom MsCoreUtils vapply1l
#' 
#' @author Johannes Rainer, Chong Tang
#'
#' @noRd
.initiate_data_to_table <- function(x, con, dbtable = "msdata") {
    basic_type <- c("integer", "numeric", "logical", "factor", "character")
    is_blob <- which(!vapply1l(x, inherits, basic_type))
    for (i in is_blob) {
        x[[i]] <- lapply(x[[i]], base::serialize, NULL)
    }
    if (!dbExistsTable(con, dbtable)) {
        x <- as.data.frame(x)
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
    x
}

#' @importFrom DBI dbAppendTable
#'
#' @noRd
.write_data_to_db <- function(x, con, dbtable = "msdata") {
    x <- .initiate_data_to_table(x, con, dbtable)
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


#' @description
#'
#' Subset the `MsBackendSqlDb` *by rows*, and store the row index of subsetting 
#' result in the slot rows.
#'
#' @param x `MsBackendSqlDb`
#'
#' @param i `integer`, `character` or `logical`.
#'
#' @return `x` with labelled index as rows.
#'
#' @author Johannes Rainer, Chong Tang
#'
#' @importFrom methods slot<-
#'
#' @noRd
.subset_backend_SqlDb <- function(x, i) {
    if (missing(i))
        return(x)
    i <- i2index(i, length(x))
    slot(x, "rows", check = FALSE) <- x@rows[i]
    x
}


#' @description
#'
#' Helper to be used in the filter functions to select the file/origin in
#' which the filtering should be performed.
#'
#' @param object `MsBackendSqlDb`
#'
#' @param dataStorage `character` or `integer` with either the names of the
#'     `dataStorage` or their rows - indices (in `unique(object$dataStorage)`) 
#'     in which the filtering should be performed.
#'
#' @param dataOrigin same as `dataStorage`, but for the `dataOrigin` spectra
#'     variable.
#'
#' @return `logical` of length equal to the number of spectra in `object`.
#'
#' @noRd
.sel_file_sql <- function(object, dataStorage = integer(), dataOrigin = integer()) {
    if (length(dataStorage)) {
        ## I do think this part can be avoid, it's not 
        ## necessary to use SQL here.
        lvls <- dbGetQuery(object@dbcon, 
                           paste0("SELECT DISTINCT dataStorage FROM ", 
                                  object@dbtable, 
                                  " where _pkey in (",
                                  toString(object@rows), ")"))
        lvls <- as.character(lvls[, 1])
        if (!(is.numeric(dataStorage) || is.character(dataStorage)))
            stop("'dataStorage' has to be either an integer with the index of",
                 " the data storage, or its name")
        if (is.numeric(dataStorage)) {
            if (dataStorage < 1 || dataStorage > length(lvls))
                stop("'dataStorage' should be an integer between 1 and ",
                     length(lvls))
            dataStorage <- lvls[dataStorage]
        }
        dataStorage(object) %in% dataStorage
    } else if (length(dataOrigin)) {
        lvls <- dbGetQuery(object@dbcon, 
                           paste0("SELECT DISTINCT dataOrigin FROM ", 
                                  object@dbtable, 
                                  " where _pkey in (",
                                  toString(object@rows), ")"))
        lvls <- as.character(lvls[, 1])
        if (!(is.numeric(dataOrigin) || is.character(dataOrigin)))
            stop("'dataOrigin' has to be either an integer with the index of",
                 " the data origin, or its name")
        if (is.numeric(dataOrigin)) {
            if (dataOrigin < 1 || dataOrigin > length(lvls))
                stop("'dataOrigin' should be an integer between 1 and ",
                     length(lvls))
            dataOrigin <- lvls[dataOrigin]
        }
        dataOrigin(object) %in% dataOrigin
    } else rep(TRUE, length(object))
}

#' Helper function to combine backends that base on [MsBackendSqlDb()]. If 
#' `dbcon` is provided, the merged `MsBackendSqlDb` instance will be initiated
#' by this `dbcon`. If `dbcon` is missing, the merged object will be stored in
#' `tempdir()` directory.
#'
#' @param objects `list` of `MsBackend` objects.
#' 
#' @param dbcon a `DBIConnection` object used to initiate a new 
#' `MsBackendSqlDb` instance, as the merged SQLite backend result.
#' 
#' @importFrom MsCoreUtils vapply1c 
#'
#' @return [MsBackendSqlDb()] object with combined content.
#'
#' @author Chong Tang
#'
#' @noRd
.combine_backend_SqlDb <- function(objects, dbcon) {
    if (length(objects) == 1)
        return(objects[[1]])
    if (!all(vapply1c(objects, class) == "MsBackendSqlDb"))
        stop("Can only merge backends of the same type: MsBackendSqlDb")
    res <- .clone_MsBackendSqlDb(objects[[1]], dbcon)
    for (i in 2:length(objects)) {
        res <- .attach_migration(res, objects[[i]])
    }
    res
}

#' Helper function for schema migration, which will use `ATTACH` statement
#' to transfer a SQLite table to another SQLite database, then use `DETACH`
#' statement to remove the attached database.
#' 
#' @param x [MsBackendSqlDb()] object will hold the migrated table.
#' 
#' @param y [MsBackendSqlDb()] object will be attached to `x`.
#' 
#' @importFrom DBI dbGetQuery dbExecute dbSendStatement dbClearResult
#' 
#' @author Chong Tang
#' 
#' @noRd
.attach_migration <- function(x, y) {
    if (!identical(spectraVariables(x) ,spectraVariables(y)))
        stop("Can only merge backends with the same spectra variables.")
    ## If `x` and `y` are sharing the same dbfile, and using the same dbtable
    if (identical(x@dbcon@dbname, y@dbcon@dbname) && 
        identical(x@dbtable, y@dbtable)) {
        x@rows <- c(x@rows, y@rows)
        x@modCount <- unlist(c(x@modCount, y@modCount),
                             use.names = FALSE)
        return(x) } else if (identical(x@dbcon@dbname, y@dbcon@dbname) && 
        (!identical(x@dbtable, y@dbtable))) {
        ## We want to know the length (row numbers) of x@dbtable
        x_length <- dbGetQuery(x@dbcon, paste0("SELECT COUNT(*) FROM ", 
                                               x@dbtable))
        x_length <- x_length[, 1]
        ## Insert y@dbtable into x@dbtable, they have the same dbcon obj
        qry <- dbSendStatement(x@dbcon, paste0("INSERT INTO ", x@dbtable, " (", 
                                               paste(spectraVariables(x), 
                                                     collapse = ", "), ") ",
                                               " SELECT ", 
                                               paste(spectraVariables(y), 
                                                     collapse = ", "), 
                                               " FROM ", y@dbtable))
        dbClearResult(qry)
        ## modify X@rows, the inserted rows will be added
        ## into the tail of x@rows
        x@rows <- unlist(c(x@rows, y@rows + x_length))
        x@modCount <- unlist(c(x@modCount, y@modCount),
                             use.names = FALSE)
        return(x) } else {
        ## While x and y have different db files.
        ## We want to know the length (row numbers) of x@dbtable
        x_length <- dbGetQuery(x@dbcon, paste0("SELECT COUNT(*) FROM ", 
                                               x@dbtable))
        x_length <- x_length[, 1]
        ## Use `ATTACH` statement to migrate y@dbtable to the db file of x
        dbExecute(x@dbcon, paste0("ATTACH DATABASE '",
                                  y@dbcon@dbname, "' AS toMerge"))
        st <- dbSendStatement(x@dbcon, paste0("insert into ", x@dbtable, " (", 
                                              paste(spectraVariables(x), 
                                              collapse = ", "), ") ",
                                        "select ", paste(spectraVariables(y), 
                                              collapse = ", "), 
                                            " from toMerge.", y@dbtable))
        suppressWarnings(dbExecute(x@dbcon, "DETACH DATABASE toMerge"))
        ## modify X@rows, the inserted rows will be added
        ## into the tail of x@rows
        x@rows <- unlist(c(x@rows, y@rows + x_length))
        x@modCount <- unlist(c(x@modCount, y@modCount),
                             use.names = FALSE)
        return(x) 
       }
}

#' Helper function to clone a `MsBackendSqlDb` instance.
#' 
#' @param x [MsBackendSqlDb()] object to be cloned.
#' 
#' @importFrom DBI dbGetQuery dbExecute dbClearResult dbSendQuery
#' 
#' @author Chong Tang
#' 
#' @noRd
.clone_MsBackendSqlDb <- function(x, dbcon) {
    ## If `cbdbcon` is missing, we will create an empty `MsBackendSqlDb` 
    ## Instance with its '.db' file in `tempdir()`.
    if (missing(dbcon) || !dbIsValid(dbcon)) {
        res <- MsBackendSqlDb()
    } else {
        res <- MsBackendSqlDb(dbcon)
    }
    slot(res, "dbtable", check = FALSE) <- x@dbtable
    slot(res, "modCount", check = FALSE) <- x@modCount
    slot(res, "rows", check = FALSE) <- x@rows
    slot(res, "columns", check = FALSE) <- x@columns
    slot(res, "readonly", check = FALSE) <- x@readonly
    slot(res, "version", check = FALSE) <- x@version
    ## If `dbtable` existed in the provided `dbcon` related database, remove it
    if (!missing(dbcon) && dbIsValid(dbcon))
        dbExecute(res@dbcon, paste0("DROP TABLE IF EXISTS ", res@dbtable))
    ## Now we want to clone the SQLite table in `x`
    if (length(x@dbtable)) 
        ## Fetch the 1st row from `x@table`, we want to fetch the column types
        row_1 <- dbGetQuery(x@dbcon, 
                            paste0("SELECT * FROM ", x@dbtable, 
                                   " WHERE _pkey = 1"))
        ## remove the column:`_pkey`
        row_1[names(row_1) %in% "_pkey"] <- NULL
        ## initiate a SQLite table in the new `MsBackendSqlDb` instance 
        ## with the same column types in x@dbtable
        .initiate_data_to_table(row_1, res@dbcon, res@dbtable)
        ## Now we copy `x@dbtable` to the SQLite database of `res`
        dbExecute(res@dbcon, paste0("ATTACH DATABASE '",
                                   x@dbcon@dbname, "' AS toMerge"))
        st <- dbSendStatement(res@dbcon, paste0("insert into ", res@dbtable,
                                                " (", 
                                   paste(spectraVariables(x), 
                                         collapse = ", "), ") ",
                                   "select ", paste(spectraVariables(x), 
                                                    collapse = ", "), 
                                   " from toMerge.", x@dbtable))
        suppressWarnings(dbExecute(res@dbcon, "DETACH DATABASE toMerge"))
        res@query <- dbSendQuery(res@dbcon, paste0("select ? from ", 
                                                   res@dbtable, 
                                                   " where _pkey = ?"))
        return(res)
}

#' Replace the values of a SQLite table column, and ensure the right data type
#'  can be returned. 
#'  I kept this function, since replacing column values in huge database is
#'  extremely time consuming. It's more efficient to use the strategy here.
#'  
#'  
#' @param x [MsBackendSqlDb()] object.
#' 
#' @param name A `character(1)` with the name of the column will be replaced.
#' 
#' @param value vector with values to replace the `name`.
#'
#' @importFrom DBI dbSendQuery dbExecute dbClearResult dbReadTable dbWriteTable dbListFields
#'
#' @noRd
.replace_db_table_columns <- function(x, name, value) {
    flds <- dbListFields(x@dbcon, "msdata")
    str1 <- dbListFields(x@dbcon, "msdata")[dbListFields(x@dbcon, 
                                                       "msdata") != name]
    sql1 <- paste0("CREATE VIEW metaview AS SELECT ",
                   toString(str1), " FROM ", x@dbtable)
    qry <- dbSendQuery(x@dbcon, sql1)
    dbClearResult(qry)
    sql2 <- paste0("CREATE VIEW metakey AS SELECT ", "_pkey",
                   " FROM ", x@dbtable)
    qry2 <- dbSendQuery(x@dbcon, sql2)
    dbClearResult(qry2)
    metapkey <- dbReadTable(x@dbcon, 'metakey')
    token <- data.frame(name = value, pkey = metapkey)
    colnames(token) <- c(name, "pkey")
    dbWriteTable(x@dbcon, 'token', token)
    sql3 <- paste0("CREATE TABLE msdata1 AS ", "SELECT * FROM metaview ",
                   "INNER JOIN token on token.pkey = metaview._pkey")
    dbExecute(x@dbcon, sql3)
    dbExecute(x@dbcon, paste0("ALTER TABLE ", x@dbtable,
                              " RENAME TO _msdata_old"))
    ## We have to sort `str2` into ordinary metadata order
    sql2 <- paste0("CREATE TABLE msdata AS SELECT ",
                   toString(flds), " FROM msdata1")
    qry2 <- dbSendQuery(x@dbcon, sql2)
    dbClearResult(qry2)
    dbExecute(x@dbcon, "DROP TABLE IF EXISTS _msdata_old")
    dbExecute(x@dbcon, "DROP TABLE IF EXISTS msdata1")
    ## Drop View
    dbExecute(x@dbcon, "DROP TABLE IF EXISTS token")
    dbExecute(x@dbcon, "DROP VIEW IF EXISTS metaview")
    dbExecute(x@dbcon, "DROP VIEW IF EXISTS metakey")
}

#' Replace the values of a SQLite table column.
#'  
#' @param x [MsBackendSqlDb()] object.
#' 
#' @param name A `character(1)` with the name of the column will be replaced.
#' 
#' @param  value vector with values to replace the `name`.
#'
#' @importFrom DBI dbExecute dbSendStatement dbWriteTable dbClearResult
#'
#' @noRd
.update_db_table_columns <- function(x, name, value) {
    table_y <- data.frame(name = value, pkey = x@rows)
    colnames(table_y) <- c(name, "pkey")
    dbWriteTable(x@dbcon, 'table_y', table_y)
    state1 <- dbSendStatement(x@dbcon, paste0("UPDATE ", x@dbtable, " SET ",
                                       name, " = (SELECT ", name, 
                                       " FROM table_y WHERE pkey = _pkey)"))
    dbExecute(x@dbcon, "DROP TABLE IF EXISTS table_y")
    x
}

#' Insert values to a SQLite table as a new column, and ensure the right data
#' type can be returned. 
#'  
#' @param x [MsBackendSqlDb()] object.
#' 
#' @param name A `character(1)` with the name of the column will be replaced.
#' 
#' @param value vector with values to replace the `name`.
#'
#' @importFrom DBI dbExecute dbSendStatement dbWriteTable dbClearResult
#'
#' @noRd
.insert_db_table_columns <- function(x, name, value) {
    newType <- dbDataType(x@dbcon, value)
    ## Use ALTER statement to insert a new column in the table
    state1 <- dbSendStatement(x@dbcon, paste0("ALTER TABLE ", x@dbtable,
                                              " ADD ", name, " ", 
                                              newType))
    table_y <- data.frame(name = value, pkey = x@rows)
    colnames(table_y) <- c(name, "pkey")
    dbWriteTable(x@dbcon, 'table_y', table_y)
    ## UPDATE the rows of new column - "name", where _pkey = x@rows
    state2 <- dbSendStatement(x@dbcon, paste0("UPDATE ", x@dbtable, " SET ",
                                              name, " = (SELECT ", name, 
                                              " FROM table_y WHERE pkey = _pkey)"))
    dbExecute(x@dbcon, "DROP TABLE IF EXISTS table_y")
    slot(x, "columns", check = FALSE) <- c(x@columns, name)
    x
}