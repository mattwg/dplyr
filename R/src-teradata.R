#' Connect to Teradata.
#'
#' Use \code{src_teradata} to connect to an existing teradata database,
#' and \code{tbl} to connect to tables within that database.
#'
#' @template db-info
#' @param dbname Database name
#' @param host,port Host name and port number of database
#' @param user,password User name and password.
#' @param ... for the src, other arguments passed on to the underlying
#'   database connector, \code{dbConnect}. For the tbl, included for
#'   compatibility with the generic, but otherwise ignored.
#' @param src a teradata src created with \code{src_teradata}.
#' @param from Either a string giving the name of table in database, or
#'   \code{\link{sql}} described a derived table or compound join.
#' @export
#' @examples
#' \dontrun{
#'
#' }
src_teradata <- function(dbname, host = NULL, port = 0L, user = "root",
                      password = "", tdjars, ...) {
  if (!require("RJDBC")) {
    stop("RJDBC package required to connect to Teradata", call. = FALSE)
  }


  # We also need Teradata jars to be included

  jars <- c(paste(tdjars,"/terajdbc4.jar",sep=""),paste(tdjars,"/tdgssconfig.jar",sep=""))
  drv <- JDBC("com.teradata.jdbc.TeraDriver",jars)

  db <- paste("jdbc:teradata://",dbname,sep="")
  con <- dbConnect(drv, db, username=user, password=password, ...)

  # call constructor for src_sql (an abstract base class) - subclassing to be object of class teradata
  src_sql("teradata", con )
}


#' @export
#' @rdname src_teradata
tbl.src_teradata <- function(src, from, ...) {
  tbl_sql("teradata", src = src, from = from, ...)
}

#' @export
brief_desc.src_teradata <- function(x) {
  print("teradata")
}

#' @export
translate_env.src_teradata <- function(x) {
  sql_variant(
    base_scalar,
    sql_translator(.parent = base_agg,
      n = function() sql("count(*)"),
      sd =  sql_prefix("stddev_pop")
    )
  )
}

# DBI methods ------------------------------------------------------------------


#' @export
db_list_tables.TeradataConnection <- function(con, table, ...) {
  dbListTables(con, ...)
}

#' @export
db_has_table.TeradataConnection <- function(con, table, ...) {
  dbExistsTable(con, name=table, ...)
}

#' @export
db_data_type.TeradataConnection <- function(con, fields, ...) {
  vapply(fields, dbDataType, dbObj = con, FUN.VALUE = character(1))
}



#
# #' @export
# db_data_type.TeradataConnection <- function(con, fields, ...) {
#   char_type <- function(x) {
#     n <- max(nchar(as.character(x), "bytes"))
#     if (n <= 65535) {
#       paste0("varchar(", n, ")")
#     } else {
#       "mediumtext"
#     }
#   }
#
#   data_type <- function(x) {
#     switch(class(x)[1],
#       logical = "boolean",
#       integer = "integer",
#       numeric = "double",
#       factor =  char_type(x),
#       character = char_type(x),
#       Date =    "date",
#       POSIXct = "datetime",
#       stop("Unknown class ", paste(class(x), collapse = "/"), call. = FALSE)
#     )
#   }
#   vapply(fields, data_type, character(1))
# }
#
# #' @export
# db_begin.TeradataConnection <- function(con, ...) {
#   dbGetQuery(con, "START TRANSACTION")
# }
#
# #' @export
# db_commit.TeradataConnection <- function(con, ...) {
#   dbGetQuery(con, "COMMIT")
# }
#
# #' @export
# db_explain.TeradataConnection <- function(con, sql, ...) {
#   exsql <- build_sql("EXPLAIN ", sql, con = con)
#   expl <- dbGetQuery(con, exsql)
#   out <- capture.output(print(expl))
#
#   paste(out, collapse = "\n")
# }
#
# #' @export
# db_insert_into.TeradataConnection <- function(con, table, values, ...) {
#
#   # Convert factors to strings
#   is_factor <- vapply(values, is.factor, logical(1))
#   values[is_factor] <- lapply(values[is_factor], as.character)
#
#   # Encode special characters in strings
#   is_char <- vapply(values, is.character, logical(1))
#   values[is_char] <- lapply(values[is_char], encodeString)
#
#   tmp <- tempfile(fileext = ".csv")
#   write.table(values, tmp, sep = "\t", quote = FALSE, qmethod = "escape",
#     row.names = FALSE, col.names = FALSE)
#
#   sql <- build_sql("LOAD DATA LOCAL INFILE ", encodeString(tmp), " INTO TABLE ",
#     ident(table), con = con)
#   dbGetQuery(con, sql)
#
#   invisible()
# }
#
# #' @export
# db_create_index.TeradataConnection <- function(con, table, columns, name = NULL,
#                                              ...) {
#   name <- name %||% paste0(c(table, columns), collapse = "_")
#   fields <- escape(ident(columns), parens = TRUE, con = con)
#   index <- build_sql("ADD INDEX ", ident(name), " ", fields, con = con)
#
#   sql <- build_sql("ALTER TABLE ", ident(table), "\n", index, con = con)
#   dbGetQuery(con, sql)
# }
#
# #' @export
# db_analyze.TeradataConnection <- function(con, table, ...) {
#   sql <- build_sql("ANALYZE TABLE", ident(table), con = con)
#   dbGetQuery(con, sql)
# }
#
# #' @export
# sql_escape_ident.TeradataConnection <- function(con, x) {
#   sql_quote(x, "`")
# }
