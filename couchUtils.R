
couch.get <- function(district,year,vdsid){

  docname = vdsid
  restful = paste(dbname,district,year,sep='%2F')
  uri=paste(couchdb,restful,docname,sep="/");
  fromJSON(getURL(uri)[[1]])

}


couch.check.is.processed <- function(district,year,vdsid){

  statusdoc = couch.get(district,year,vdsid)
  result <- FALSE ## doc needs to be processed
  fieldcheck <- c('error','processed') %in% names(statusdoc)
  if(!fieldcheck[1] && fieldcheck[2]){
    result <- TRUE
  }
  result

}

couch.put <- function(district,year,vdsid,doc){

  restful = paste(dbname,district,year,sep='%2F')
  uri=paste(couchdb,restful,vdsid,sep="/");

  reader = basicTextGatherer()

  curlPerform(
              url = uri
              ,httpheader = c('Content-Type'='application/json')
              ,customrequest = "PUT"
              ,postfields = toJSON(doc)
              ,writefunction = reader$update
              )

  reader$value()
}

couch.save.is.processed <- function(district,year,vdsid,doc=list(processed=1)){

  current = couch.get(district,year,vdsid)
  if( c('_rev') %in% names(current) ){
    doc = merge (current,doc)
  }
  couch.put(district,year,vdsid,doc)

}
