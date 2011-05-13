## requires that dbname be set externally
couchenv = Sys.getenv(c("COUCHDB_HOST", "COUCHDB_USER", "COUCHDB_PASS"))
couchdb = paste("http://",couchenv[1],":5984",sep='')


couch.makedbname <- function( components ){
  tolower(paste(components,collapse='%2F'))
}


couch.makedb <- function( db ){

  if(length(db)>0){
    db <- couch.makedbname(db)
  }

  privcouchdb = paste("http://",couchenv[2],":",couchenv[3],"@",couchenv[1],":5984",sep='')
  uri=paste(privcouchdb,db,sep="/");

  reader = basicTextGatherer()

  curlPerform(
              url = uri
              ,httpheader = c('Content-Type'='application/json')
              ,customrequest = "PUT"
              ,writefunction = reader$update
              )

  reader$value()
}

couch.get <- function(db,doc){

  if(length(db)>0){
    db <- couch.makedbname(db)
  }
  uri=paste(couchdb,db,doc,sep="/");
  fromJSON(getURL(uri)[[1]])

}

couch.put <- function(db,docname,doc){

  if(length(db)>0){
    db <- couch.makedbname(db)
  }

  uri=paste(couchdb,db,docname,sep="/");

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


couch.check.is.processed <- function(district,year,vdsid){

  statusdoc = couch.get(c(district,year),vdsid)
  result <- TRUE ## default to done
  fieldcheck <- c('error','inprocess','processed') %in% names(statusdoc)
  if(fieldcheck[1] && !fieldcheck[2] && !fieldcheck[3]){
    putstatus <- fromJSON(couch.put(c(district,year),vdsid,list(inprocess=1)))
    fieldcheck <- c('error') %in% names(putstatus)
    if(!fieldcheck[1]){
      result <- FALSE
    }
  }
  result

}


couch.save.is.processed <- function(district,year,vdsid,doc=list(processed=1)){

  current = couch.get(c(district,year),vdsid)
  doc = merge (current,doc)
  couch.put(c(district,year),vdsid,doc)

}

couch.bulk.docs.save <- function(district,year,vdsid,docs){

  ## assume here (because I am lazy) that docs is a list of json encoded records, one per doc

  ## push 1000 at a time
  i = 10000
  if(i > length(docs)) i = length(docs)

  j = 1

  while(j < length(docs) ) {

    bulkdocs = paste('{"docs":[',paste(docs[j:i], collapse=','),']}',sep='')

    j = i+1;
    i = j + 10000;
    if(i > length(docs)) i = length(docs)

    ## form the URI for the call

    ## first the db name
    restful = paste(dbname,district,year,sep='%2F')

    ## then the bulk docs target
    uri=paste(couchdb,restful,'_bulk_docs',sep="/")

    ## use the simple callback mechanism
    reader = basicTextGatherer()

    curlPerform(
                url = uri
                ,httpheader = c('Content-Type'='application/json')
                ,customrequest = "POST"
                ,postfields = bulkdocs
                ,writefunction = reader$update
                )
    ## print(reader$value()
  }

}

