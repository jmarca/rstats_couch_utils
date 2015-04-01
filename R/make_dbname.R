##' preface a vector of name pieces with the default preface or
##' namespace or whatever
##'
##' CouchDB databases can get messy.  This lets you specify a value called "dbname" in the config file and all of the database names you create here with the following functions will be prefaced with this value.  So you can call it "test" for testing cases, etc.
##'
##' @title couch.preface
##' @param components a vector of parts of the name
##' @return an extended list of components, with the new first element
##' being the config$dbname value.  If that value is null, then you
##' get back what you passed in.
##' @author James E. Marca
couch.preface <- function(components){
    config <- get.config()
    if(!is.null(config$dbname)){
        components <- c(config$dbname,components)
    }
    components
}


##' make database name, given some inputs
##'
##' really just separates things by %2f, which is ascii for / Also,
##' due to history, it currently will use "dbname" to sort of
##' namespace all the created directories in couchdb
##'
##' @title couch.makedbname
##' @param components the parts of the name to be joined with
##' ascii-escaped slashes
##' @return  the created name string
##' @export
##' @author James E. Marca
couch.makedbname <- function( components ){
    tolower(paste(couch.preface(components),collapse='%2F'))
}

## probably not going to be used anymore, so comment it out and see
trackingdb <- "evil global variable" ## couch.makedbname('tracking')

##' Make CouchDB database name, but without escaping the slashes
##'
##' This is similar to the above, but instead of using %2f for the
##' slash, just use the slash.  This is useful for things like
##' creating replications, etc Also good for printing out the database
##' name without irritating the reader, or sending to a filesystem
##' instead of couchdb?
##'
##' @title couch.makedbname.noescape
##' @param components the parts of the name that will be joined with slashes and prefaced with config$couchdb$dbname (which defaults to 'vdsdata')
##' @return the created name string
##' @export
##' @author James E. Marca
couch.makedbname.noescape <- function( components ){
    tolower(paste(couch.preface(components),collapse='/'))
}
