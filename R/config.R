## requires that dbname, etc be set externally
## non-local is where to go to be sure (tracking db)
## and also the target of all replication calls
## local is where to send bulk saves,
## and the source of replication calls


config <- NULL

##' Read in the config file, return as an object
##'
##' depends on the environment variable R_COUCH_CONFIG_FILE
##' and presumes it is JSON and presumes that it has an entry called couchdb.
##'
##' So for example
##'
##' {
##'     "couchdb": {
##'         "host": "192.168.0.1",
##'         "port":5984,
##'         "trackingdb":"a_test_state_db",
##'         "auth":{"username":"blabbity",
##'                 "password":"correct horse battery staple"
##'                }
##'     },
##'     "postgresql":{
##'         "host":"192.168.0.1",
##'         "port":5432,
##'         "auth":{"username":"sqlrulez"},
##'         "grid_merge_sqlquery_db":"spatialspaces"
##'     }
##' }
##'
##' Note that the "couchdb" portion of your configuration should
##' follow that pattern exactly.  I really want host, port, trackingdb
##' and auth, with auth containing username and password fields
##'
##' @title read.config
##' @return the config file, as an R object, but only the couchdb part
##' @author James E. Marca
get.config <- function(){
    if(is.null(config)){

        configfile <-  Sys.getenv(c("RCOUCHDBUTILS_CONFIG_FILE"))[1]
        if(configfile ==  ''){
            configfile <- 'test.config.json'
        }

        config <- rjson::fromJSON(file=configfile)$couchdb

        if(is.null(config$host)){
            config$host <- '127.0.0.1'
        }
        if(is.null(config$port)){
            config$port <- 5984
        }
        if(is.null(config$trackingdb)){
            trackingdb <- 'tracking'
        }

        if(is.null(config$auth)){
            print ('warning, without an auth field in the config JSON you will not be able to create or delete databases')

        }else{
            if(is.null(config$auth$username)){
                print ('warning, without an auth:username field in the config JSON you will not be able to create or delete databases')
            }
            if(is.null(config$auth$password)){
                print ('warning, without an auth:password field in the config JSON you will not be able to create or delete databases')
            }
        }

        ## set dbname default, if not specified
        if(is.null(config$dbname)){
            config$dbname <- 'vdsdata'
        }
        options(RCurlOptions = list(
                    fresh.connect = TRUE
            ))
    }
    config
}

##' get the couchdb url based on the config file parameters
##'
##' @title couch.get.url
##' @return a URL to couchdb
##' @author James E. Marca
couch.get.url <- function(){
    config <- get.config()
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
    config <- get.config()
    authstring <- NULL
    if(!is.null(config$auth) &&
       !is.null(config$auth$username) &&
       !is.null(config$auth$password)){
        authstring <- paste(config$auth$username,config$auth$password,sep=':')
    }
    return (authstring)
}
