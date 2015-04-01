##' Post a document to couchdb database.
##'
##' @title couch.post
##' @param db the target database
##' @param doc the document to post
##' @param h a curl handle, if you have a persistent one
##' @return the response from couchdb.  Probably okay or not okay kind
##' of thing, parsed JSON
##' @export
##' @author James E. Marca
couch.post <- function(db,doc,h=RCurl::getCurlHandle()){
  if(length(db)>1){
    db <- couch.makedbname(db)
  }
  couchdb <-  couch.get.url()
  uri <- paste(couchdb,db,sep="/");
  couch_userpwd <- couch.get.authstring()

  reader = RCurl::basicTextGatherer()
  if(is.null(couch_userpwd)){
      RCurl::curlPerform(
          url = uri
         ,customrequest = "POST"
         ,httpheader = c('Content-Type'='application/json')
         ,postfields = RJSONIO::toJSON(doc,collapse='')
         ,writefunction = reader$update
         ,curl=h
          )
  }else{
      RCurl::curlPerform(
          url = uri
         ,customrequest = "POST"
         ,httpheader = c('Content-Type'='application/json')
         ,postfields = RJSONIO::toJSON(doc,collapse='')
         ,writefunction = reader$update
         ,curl=h


         ,userpwd=couch_userpwd
          )
  }
  RJSONIO::fromJSON(reader$value(),simplify=FALSE)
}

##' Get a named document from couchdb database.
##'
##' @title couch.get
##' @param db the target database
##' @param docname the document id to fetch
##' @param h a curl handle, if you have a persistent one
##' @return the response from couchdb.  Probably okay or not okay kind
##' of thing, parsed JSON
##' @export
##' @author James E. Marca
couch.get <- function(db,docname, h=RCurl::getCurlHandle()){

  if(length(db)>1){
    db <- couch.makedbname(db)
  }
  couchdb <-  couch.get.url()
  uri <- paste(couchdb,db,docname,sep="/");

  ## remove spaces in url or doc id
  uri <- gsub("\\s","%20",x=uri,perl=TRUE)

  couch_userpwd <- couch.get.authstring()

  res <- NULL
  if(is.null(couch_userpwd)){
      res <- RJSONIO::fromJSON(
          RCurl::getURL(uri,curl=h)[[1]],
          simplify=FALSE
          )
  }else{
      res <- RJSONIO::fromJSON(
          RCurl::getURL(uri,curl=h,userpwd=couch_userpwd)[[1]],
          simplify=FALSE
          )
  }
  res
}

##' HEAD on a named document from couchdb database.
##'
##' @title couch.head
##' @param db the target database
##' @param docname the document id to fetch
##' @param h a curl handle, if you have a persistent one
##' @return the response from couchdb.  Probably okay or not okay kind
##' of thing, parsed JSON
##' @export
##' @author James E. Marca
couch.head <- function(db,docname, h=RCurl::getCurlHandle()){

  if(length(db)>1){
    db <- couch.makedbname(db)
  }
  couchdb <-  couch.get.url()
  uri <- paste(couchdb,db,docname,sep="/");

  ## remove spaces in url or doc id
  uri <- gsub("\\s","%20",x=uri,perl=TRUE)

  couch_userpwd <- couch.get.authstring()

  reader <- RCurl::basicHeaderGatherer()
    if(is.null(couch_userpwd)){

      RCurl::getURLContent(
          url = uri
         ,customrequest = "HEAD"
         ,nobody=TRUE
         ,httpheader = c('Content-Type'='application/json')
         ,headerfunction = reader$update
         ,curl=h
          )

  }else{

      RCurl::getURLContent(
          url = uri
         ,customrequest = "HEAD"
         ,nobody=TRUE
         ,httpheader = c('Content-Type'='application/json')
         ,headerfunction = reader$update
         ,curl=h
         ,userpwd=couch_userpwd
          )

  }

  reader$value()
}


##' Put a named document into the couchdb database.
##'
##' @title couch.put
##' @param db the target database
##' @param docname the document id to put
##' @param doc the document to save at the above id
##' @param h a curl handle, if you have a persistent one
##' @param dumper the JSON dumper that works best with your doc
##' @return the response from couchdb.  Probably okay or not okay kind
##' of thing, parsed JSON
##' @export
##' @author James E. Marca
couch.put <- function(db,docname,doc,h=RCurl::getCurlHandle(),dumper=jsondump5){

  if(length(db)>1){
    db <- couch.makedbname(db)
  }
  couchdb <-  couch.get.url()
  uri <- paste(couchdb,db,docname,sep="/")

  couch_userpwd <- couch.get.authstring()

  ## remove spaces in url or doc id
  uri <- gsub("\\s","%20",x=uri,perl=TRUE)

  reader = RCurl::basicTextGatherer()

  if(is.null(couch_userpwd)){
      RCurl::curlPerform(
          url = uri
         ,customrequest = "PUT"
         ,httpheader = c('Content-Type'='application/json')
         ,postfields = dumper(doc)
         ,writefunction = reader$update
         ,curl=h
          )
  }else{
      RCurl::curlPerform(
          url = uri
         ,customrequest = "PUT"
         ,httpheader = c('Content-Type'='application/json')
         ,postfields = dumper(doc)
         ,writefunction = reader$update
         ,curl=h
         ,userpwd=couch_userpwd
          )

  }
  RJSONIO::fromJSON(reader$value(),simplify=FALSE)
}

##' Delete a named document from couchdb database.
##'
##' @title couch.delete
##' @param db the target database
##' @param docname the document id to fetch
##' @param doc the existing document.  if it doesn't exist, then a
##' head will be performed to grab the current revision.  If you have
##' the current doc laying around this will save a round trip to the
##' db.  However, if the revision in your doc is out of date, then the
##' delete will fail.  This can be good or bad, depending on your
##' point of view.
##' @param h a curl handle, if you have a persistent one
##' @return the response from couchdb.  Probably okay or not okay kind
##' of thing, parsed JSON
##' @export
##' @author James E. Marca
couch.delete <- function(db,docname,doc=NULL,h=RCurl::getCurlHandle()){

  if(length(db)>1){
    db <- couch.makedbname(db)
  }
  couchdb <-  couch.get.url()
  uri <- paste(couchdb,db,docname,sep="/")
  couch_userpwd <- couch.get.authstring()

  doc_rev <- NULL
  if(is.null(doc) || is.null(doc['_rev'])){
      ## do a head fetch to get the rev
      doc.head <- couch.head(db,docname,h)
      doc_rev <- gsub('\\"','',x=doc.head['ETag'],perl=TRUE)
  }else{
      doc_rev <- doc['_rev']
  }
  uri <- paste(uri,paste('rev',doc_rev,sep='='),sep='?')

  ## remove spaces in url or doc id
  uri <- gsub("\\s","%20",x=uri,perl=TRUE)
  reader = RCurl::basicTextGatherer()

  if(is.null(couch_userpwd)){
      RCurl::curlPerform(
          url = uri
         ,httpheader = c('Content-Type'='application/json')
         ,customrequest = "DELETE"
         ,writefunction = reader$update
          )
  }else{
      RCurl::curlPerform(
          url = uri
         ,httpheader = c('Content-Type'='application/json')
         ,customrequest = "DELETE"
         ,writefunction = reader$update
         ,userpwd=couch_userpwd
          )
  }

  RJSONIO::fromJSON(reader$value(),simplify=FALSE)
}

couch.allDocs <- function(db, query, view='_all_docs', include.docs = TRUE, local=TRUE, h=getCurlHandle()){

  if(length(db)>1){
    db <- couch.makedbname(db)
  }
  cdb <- localcouchdb
  if(!local){
    cdb <- couchdb
  }
  ## docname <- '_all_docs'
  uri <- paste(cdb,db,view,sep="/");
##   print(uri)
  q <- paste(names(query),query,sep='=',collapse='&')
  q <- gsub("\\s","%20",x=q,perl=TRUE)
  q <- gsub('"',"%22",x=q,perl=TRUE)
  if(include.docs){
    q <- paste(q,'include_docs=true',sep='&')
  ## }else{
  ##   q <- paste(q,sep='&')
  }
  print (paste(uri,q,sep='?'))
  reader <- basicTextGatherer()
  curlPerform(
              url = paste(uri,q,sep='?')
              ,customrequest = "GET"
              ,httpheader = c('Content-Type'='application/json')
              ,writefunction = reader$update
              ,curl=h
              )
  fromJSON(reader$value()[[1]],simplify=FALSE)
}
