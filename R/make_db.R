
couch.makedb <- function( db, local=TRUE ){

    if(length(db)>1){
        db <- couch.makedbname(db)
    }

    couchdb <-  couch.get.url()
    uri <- paste(couchdb,db,sep="/");
    couch_userpwd <- couch.get.authstring()

    ##  print(uri)

    reader = RCurl::basicTextGatherer()

    RCurl::curlPerform(
        url = uri
       ,httpheader = c('Content-Type'='application/json')
       ,customrequest = "PUT"
       ,writefunction = reader$update
       ,userpwd=couch_userpwd
        )

    ##print(paste( 'making db',db, reader$value() ))
    rjson::fromJSON(reader$value())
}

##' Delete a CouchDB database
##'
##' @title couch.deletedb
##' @param db the database name to delete.  If you pass a plain
##' string, then it is your responsibility to make sure to escape the
##' / to be %2f.  However, the advantage is that you bypass my
##' namespacing db naming thing.  If you *want* the database
##' namespacing trick, then pass in a vector of name parts, like
##' c('my','current','database') and it wil be passed to the
##' \code{\link{couch.makedbname}} function, where the current value
##' of \code{config.dbname} will be prepended to the list, giving you
##' a database name like "vdsdata%2fmy%2fcurrent%2fdatabase"
##' @return it will either delete the database, or curl will crash
##' @export
##' @author James E. Marca
couch.deletedb <- function(db){

  if(length(db)>1){
    db <- couch.makedbname(db)
  }

  couchdb <-  couch.get.url()
  uri <- paste(couchdb,db,sep="/");
  couch_userpwd <- couch.get.authstring()

  reader = RCurl::basicTextGatherer()
  RCurl::curlPerform(
      url = uri
     ,httpheader = c('Content-Type'='application/json')
     ,customrequest = "DELETE"
     ,writefunction = reader$update
     ,userpwd=couch_userpwd
      )

  rjson::fromJSON(reader$value())
}
