##' make a new couchdb
##'
##' @title couch.makedb
##' @param db the db name
##' @return the JSON response from the CouchDB server, as a list
##' @export
##' @author James E. Marca
couch.makedb <- function( db){

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
##' This is what you use to delete a couchdb database.  I usually just
##' use it to clean up after tests, but maybe you want a temporary db
##' during an analysis or somethign
##'
##' @title couch.deletedb
##' @param db the database name to delete.  If a list, it will get
##' passed into couch.makedbname to make into a proper db name.
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
