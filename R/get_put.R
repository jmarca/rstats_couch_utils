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
  uri <- paste(couchdb,db,sep="/")
  couch_userpwd <- couch.get.authstring()

  reader = RCurl::basicTextGatherer()
  if(is.null(couch_userpwd)){
      RCurl::curlPerform(
          url = uri
         ,customrequest = "POST"
         ,httpheader = c('Content-Type'='application/json')
         ,postfields = rjson::toJSON(doc)
         ,writefunction = reader$update
         ,curl=h
          )
  }else{
      RCurl::curlPerform(
          url = uri
         ,customrequest = "POST"
         ,httpheader = c('Content-Type'='application/json')
         ,postfields = rjson::toJSON(doc)
         ,writefunction = reader$update
         ,curl=h
         ,userpwd=couch_userpwd
          )
  }
  rjson::fromJSON(reader$value())
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
  uri <- paste(couchdb,db,
               ## remove spaces in url or doc id
               RCurl::curlEscape(docname),
               sep="/");

  couch_userpwd <- couch.get.authstring()

  res <- NULL
  if(is.null(couch_userpwd)){
      res <- rjson::fromJSON(
          RCurl::getURL(uri,curl=h)[[1]]
          )
  }else{
      res <- rjson::fromJSON(
          RCurl::getURL(uri,curl=h,userpwd=couch_userpwd)[[1]]
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
  uri <- paste(couchdb,db,
               ## remove spaces in url or doc id
               RCurl::curlEscape(docname),
               sep="/");

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
  uri <- paste(couchdb,db,
               ## remove spaces in url or doc id
               RCurl::curlEscape(docname),
               sep="/");
  couch_userpwd <- couch.get.authstring()
  reader = RCurl::basicTextGatherer()

  jsondoc <- dumper(doc)
  if(is.character(doc)){
      jsondoc <- doc
  }
  if(is.null(couch_userpwd)){
      RCurl::curlPerform(
          url = uri
         ,customrequest = "PUT"
         ,httpheader = c('Content-Type'='application/json')
         ,postfields = jsondoc
         ,writefunction = reader$update
         ,curl=h
          )
  }else{
      RCurl::curlPerform(
          url = uri
         ,customrequest = "PUT"
         ,httpheader = c('Content-Type'='application/json')
         ,postfields = jsondoc
         ,writefunction = reader$update
         ,curl=h
         ,userpwd=couch_userpwd
          )

  }
  response <-  reader$value()
  if(!is.null(response) && response != ''){
      response <- rjson::fromJSON(response)
  }
  response
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
##' @return 1 if delete was successful or the doc was not found, 0
##' otherwise.  So zero is bad, 1 is good
##' @export
##' @author James E. Marca
couch.delete <- function(db,docname,doc=NULL){

    h=RCurl::getCurlHandle()
    couch.session(h)## session is required to delete replication docs

    if(length(db)>1){
        db <- couch.makedbname(db)
    }
    couchdb <-  couch.get.url()
    uri <- paste(couchdb,db,
                 ## remove spaces in url or doc id
                 RCurl::curlEscape(docname),
                 sep="/");


    couch_userpwd <- couch.get.authstring()

    doc_rev <- NULL
    if(is.null(doc) || is.null(doc['_rev'])){
        ## do a head fetch to get the rev
        doc_rev <- get.rev.from.head(db,docname,h)
    }else{
        doc_rev <- doc['_rev']
    }
    uri <- paste(uri,paste('rev',doc_rev,sep='='),sep='?')
    hreader <- RCurl::basicHeaderGatherer()
    breader = RCurl::basicTextGatherer()
    RCurl::curlPerform(
        url = uri
       ,httpheader = c('Content-Type'='application/json')
       ,customrequest = "DELETE"
       ,headerfunction = hreader$update
       ,writefunction = breader$update
       ,curl=h
       ##,verbose=TRUE
        )

    headers <-  hreader$value()
    not.found <- headers['status'] == 404
    del.okay  <- headers['status'] == 200
    if(any(not.found) || any(del.okay)){
        return(1)
    }else{
        return(0)
    }

}
##' a convenience wrapper around the head call, above
##'
##' @title get.rev.from.head
##' @param db the database name
##' @param docname the document name
##' @param h the current curl handle, or will gen a new one
##' @return a string value of the current doc's revision
##' @author James E. Marca
get.rev.from.head <- function(db,docname,h=RCurl::getCurlHandle()){
    doc.head <- couch.head(db,docname,h)
    doc_rev <- gsub('\\"','',x=doc.head['ETag'],perl=TRUE)
    doc_rev
}

##' a convenience for poking a database
##'
##' gets information about the database.  from the couchdb docs:
##'
##' Response JSON Object:
##'
##' \describe{
##'     \item{committed_update_seq (number)}{ The number of committed update.}
##'     \item{compact_running (boolean)}{ Set to true if the database compaction routine is operating on this database.}
##'     \item{db_name (string)}{ The name of the database.}
##'     \item{disk_format_version (number)}{ The version of the physical format used for the data when it is stored on disk.}
##'     \item{data_size (number)}{ Actual data size in bytes of the database data.}
##'     \item{disk_size (number)}{ Size in bytes of the data as stored on the disk. Views indexes are not included in the calculation.}
##'     \item{doc_count (number)}{ A count of the documents in the specified database.}
##'     \item{doc_del_count (number)}{ Number of deleted documents}
##'     \item{instance_start_time (string)}{ Timestamp of when the database was opened, expressed in microseconds since the epoch.}
##'     \item{purge_seq (number)}{ The number of purge operations on the database.}
##'     \item{update_seq (number)}{ The current number of updates to the database.}
##' }
##'
##' @title get.db
##' @param db the database name
##' @param h the current curl handle, or will gen a new one
##' @return the converted JSON status of the database
##' @author James E. Marca
get.db <- function(db,h=RCurl::getCurlHandle()){
  if(length(db)>1){
    db <- couch.makedbname(db)
  }
  couchdb <-  couch.get.url()
  uri <- paste(couchdb,db,sep="/")

  res <- rjson::fromJSON(
      RCurl::getURL(uri,curl=h)[[1]]
      )

  res
}



##' Set state to a passed in document (a list), which is more robust
##' that the dumb checkout for processing version in couchUtils.R
##'
##' All of the list items will be added to the doc:year:{} as new
##' items in teh JSON doc
##'
##' @title couch.set.state
##' @param year the year
##' @param id the detector id (VDS or WIM or whatever type detector)
##' @param doc the list() of named values to add to the JSON subpart
##' under the year
##' @param h a prior curl handle, or will automatically get a new one
##' @param db the name of the state database
##' @return the result of the call to \code{\link{couch.put}}
##' @export
##' @author James E. Marca
couch.set.state <- function(year,id, doc,
                            h=RCurl::getCurlHandle(),
                            db){

  current = couch.get(db,id,h=h)
  doc.names  <- names(doc)
  current.names <- names(current)
  if('error' %in% current.names && length(current.names) == 2){
      ## error means there isn't a current document in the db
      current = list()
  }
  ## R doesn't interpolate variables in statements like
  ## list(doc.names=doc)
  current[[paste(year)]][doc.names] = doc
  ## clean mess
  if('error' %in% current.names && length(current.names) > 2){
    current[['error']] <- NULL
    current[['reason']] <- NULL
  }
  couch.put(db,id,current,h=h)

}
##' Check the state of the given detector on the given year
##'
##' Basic idea is to use CouchDB as a sort of state database for each
##' detector.  Stash in a doc (identified by the detector's id)
##' everything that I know about the detector, by year.
##'
##' @title couch.check.state
##' @param year the year for this state bit
##' @param id the id of the detector.  Can be a VDS id or a WIM id, or
##' any other detector id that is unique.  For example, for WIM
##' detectors I add on the direction and the letters "wim", as in
##' "wim.10.N"
##' @param state the new state to stash in the detector's doc, for the
##' given year
##' @param db the state db
##' @return either the status, if found, or "todo" if not
##' @export
##' @author James E. Marca
couch.check.state <- function(year,id,state,db){
  statusdoc <- couch.get(db,id)
  result <- 'error' ## default to error
  current.names <- names(statusdoc)
  if('error' %in% current.names && length(names)==2){
    result <- 'todo'
  }else{
    fieldcheck <- c(state) %in% names(statusdoc[[paste(year)]])
    if(!fieldcheck[1] ){
      ## either no status doc, or no recorded state for this state, mark as 'todo'
      result <- 'todo'
    }else{
      result <- statusdoc[[paste(year)]][[state]]
    }
  }
  result
}
