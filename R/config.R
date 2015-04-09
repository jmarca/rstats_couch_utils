

##' wrapper for configr::configrr closure generator
##'
##' assumes the file passed in is JSON.  See configr::configrr
##'
##' @param file the file to parse. no default.  no safetynet
##' @return the configuration, or nothing, I guess
configrr <- configr::configrr()

##' Get the configuration file and load it up
##'
##' assumes the file passed in is JSON.  See configr::configrr
##'
##' Also, you need to call this before doing anything else at least
##' once with an actual configuration file.  Every other time it will
##' just grab the results of the very first invocation.
##'
##' @title get.config
##' @param file the file to parse. no default.  no safetynet
##' @return the configuration, or nothing, I guess
##' @author James E. Marca
##' @export
get.config <- function(file){
    configrr(file)
}


##' get the couchdb url based on the config file parameters
##'
##' @title couch.get.url
##' @return a URL to couchdb
##' @export
##' @author James E. Marca
##'
couch.get.url <- function(){
    config <- get.config()$couchdb
    couchdb <- paste("http://",config$host,':',config$port,sep='')
    return (couchdb)
}

##' Return the authstring needed for RCurl options, based on entries
##' in config JSON
##'
##' @title couch.get.authstring
##' @return an auth string you can use with RCurl call
##' @author James E. Marca
couch.get.authstring <- function(){
    config <- get.config()$couchdb
    authstring <- NULL
    if(!is.null(config$auth) &&
       !is.null(config$auth$username) &&
       !is.null(config$auth$password)){
        authstring <- paste(config$auth$username,config$auth$password,sep=':')
    }
    return (authstring)
}
