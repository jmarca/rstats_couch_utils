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
##'                },
##'         "design":"detectors",
##'         "view":"fips_year"
##'     },
##'     "postgresql":{
##'         "host":"192.168.0.1",
##'         "port":5432,
##'         "auth":{"username":"sqlrulez"},
##'         "grid_merge_sqlquery_db":"spatialspaces"
##'     }
##' }
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

        config <- RJSONIO::fromJSON(content=configfile,
                                    allowComments=FALSE)$couchdb

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
