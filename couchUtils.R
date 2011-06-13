## requires that dbname be set externally
couchenv = Sys.getenv(c("COUCHDB_HOST", "COUCHDB_USER", "COUCHDB_PASS", "COUCHDB_PORT"))
couchdb = paste("http://",couchenv[1],":",couchenv[4],sep='')

## null reader for RCurl when bulk saving

nullTextGatherer <-
  #
  # This is a function that is used to create a closure (i.e. a function with its own local variables
  # whose values persist across invocations).  This is called to provide an instance of a function that is
  # called when the libcurl engine has some text to be processed as it is reading the HTTP response from the
  # server.
  # The function that reads the text can do whatever it wants with it. This one simply
  # cumulates it and makes it available via a second function.
  #
function(txt = character(), max = NA, value = NULL)
{
  update = function(str) {
    ## let the prior string spill onto floor
    txt <<-   c(txt)
    nchar(str, "bytes") # use bytes rather than chars as for UTF-8, etc. we may have fewer characters,
                        # but the C code for libcurl works in bytes. If we report chars and < bytes,
                        # libcurl terminates the download.
  }

  reset = function() {  txt <<- character() }

  val = if(missing(value))
            function(collapse="", ...) {
                         if(is.null(collapse))
                             return(txt)

                         paste(txt, collapse = collapse, ...)
            }
        else
          function() value(txt)


  ans = list(update = update,
             value = val,
             reset = reset)

  class(ans) <- c("RCurlTextHandler", "RCurlCallbackFunction")

  ans$reset()

  ans
}


couch.makedbname <- function( components ){
  if(!is.null(dbname)){
    components <- c(dbname,components)
  }
  tolower(paste(components,collapse='%2F'))
}


couch.makedb <- function( db ){

  if(length(db)>0){
    db <- couch.makedbname(db)
  }
  # print(paste('making db',db))
  privcouchdb = paste("http://",couchenv[2],":",couchenv[3],"@",couchenv[1],":",couchenv[4],sep='')
  uri=paste(privcouchdb,db,sep="/");

  reader = basicTextGatherer()

  curlPerform(
              url = uri
              ,httpheader = c('Content-Type'='application/json')
              ,customrequest = "PUT"
              ,writefunction = reader$update
              )

  print(paste( 'making db',db, reader$value() ))
  reader$value()
}

couch.deletedb <- function(db){

  if(length(db)>0){
    db <- couch.makedbname(db)
  }

  privcouchdb = paste("http://",couchenv[2],":",couchenv[3],"@",couchenv[1],":",couchenv[4],sep='')
  uri=paste(privcouchdb,db,sep="/");

  reader = basicTextGatherer()
  curlPerform(
              url = uri
              ,httpheader = c('Content-Type'='application/json')
              ,customrequest = "DELETE"
              ,writefunction = reader$update
              )

  print(paste('deleted db',db,reader$value(),collapse=' '))
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

couch.delete <- function(db,docname,doc){

  if(length(db)>0){
    db <- couch.makedbname(db)
  }

  uri=paste(couchdb,db,docname,sep="/");
  uri=paste(uri,paste('rev',doc['_rev'],sep='='),sep='?')
  reader = basicTextGatherer()

  curlPerform(
              url = uri
              ,httpheader = c('Content-Type'='application/json')
              ,customrequest = "DELETE"
              ,writefunction = reader$update
              )

  reader$value()
}



couch.check.is.processed <- function(district,year,vdsid,deldb=TRUE){

  statusdoc = couch.get(c(district,year),vdsid)
  result <- TRUE ## default to done
  fieldcheck <- c('error','inprocess','processed') %in% names(statusdoc)
  ## print(fieldcheck)
  ## if(fieldcheck[2] && !fieldcheck[4]){
  ##   ## yet another temporary fix for a big screw up
  ##   couch.delete(c(district,year),vdsid,statusdoc)
  ##   statusdoc = couch.get(c(district,year),vdsid)
  ##   result <- TRUE ## default to done
  ##   fieldcheck <- c('error','inprocess','processed') %in% names(statusdoc)
  ## }
  if(fieldcheck[1] && !fieldcheck[2] && !fieldcheck[3]){
    putstatus <- fromJSON(couch.put(c(district,year),vdsid,list('inprocess'=1)))
    fieldcheck <- c('error') %in% names(putstatus)
    if(!fieldcheck[1]){
      ## did not get an error on write, so I set 'inprocess'
      ## okay, my db, okay to break things
      result <- FALSE
      if(deldb) couch.deletedb(c(district,year,vdsid))
    }
  }
  result

}


couch.save.is.processed <- function(district,year,vdsid,doc=list(processed=1)){

  current = couch.get(c(district,year),vdsid)
  doc = merge (current,doc)
  couch.put(c(district,year),vdsid,doc)

}

##################################################
## generalization of the above
##################################################
couch.check.state <- function(district,year,vdsid,process){
  statusdoc = couch.get(c(district,year),vdsid)
  result <- 'error' ## default to error
  fieldcheck <- c('error',process) %in% names(statusdoc)
  if( (fieldcheck[1] && statusdoc$error == "not_found") ||
     !fieldcheck[2] ){
    ## either no status doc, or no recorded state for this process, mark as 'todo'
    result <- 'todo'
  }else{
    result <- statusdoc[process]
  }
  result
}

couch.checkout.for.processing <- function(district,year,vdsid,process){
  result <- 'done' ## default to done
  statusdoc = couch.get(c(district,year),vdsid)
  fieldcheck <- c('error',process) %in% names(statusdoc)
  if( (fieldcheck[1] && statusdoc$error == "not_found") ){
    result = 'todo'
    statusdoc = list() ## R doesn't interpolate variables in statements like list(process='state')
    statusdoc[process]='inprocess';
    putstatus <- fromJSON(couch.put(c(district,year),vdsid,statusdoc))
    fieldcheck <- c('error') %in% names(putstatus)
    if(!fieldcheck[1]){
      result <- 'error'
    }

  }else if( !fieldcheck[2] ||  statusdoc[process] == 'todo' ){
    result = 'todo'
    statusdoc[process]='inprocess';
    putstatus <- fromJSON(couch.put(c(district,year),vdsid,statusdoc))
    fieldcheck <- c('error') %in% names(putstatus)
    if(!fieldcheck[1]){
      result <- 'error'
    }
  }else{
    ## have status doc, process field, but not in 'todo' state, so report what state it is in
    result = statusdoc[process]
  }
  result
}
#########

couch.bulk.docs.save <- function(district,year,vdsid,docs){

  ## assume here (because I am lazy) that docs is a list of json encoded records, one per doc

  ## push 1000 at a time
  i = 10000
  if(i > length(docs)) i = length(docs)

  j = 1

  db <- couch.makedbname(c(district,year,vdsid))

  couch.makedb(c(district,year,vdsid))


  while(j < length(docs) ) {

    bulkdocs = paste('{"docs":[',paste(docs[j:i], collapse=','),']}',sep='')

    j = i+1;
    i = j + 10000;
    if(i > length(docs)) i = length(docs)

    ## form the URI for the call

    ## then the bulk docs target
    uri=paste(couchdb,db,'_bulk_docs',sep="/")
    #print(paste('Saving docs to ', uri ))
    ## use the simple callback mechanism
    reader = nullTextGatherer()

    curlPerform(
                url = uri
                ,httpheader = c('Content-Type'='application/json')
                ,customrequest = "POST"
                ,postfields = bulkdocs
                ,writefunction = reader$update
                )

  }

}

library('plyr')

couch.async.bulk.docs.save <- function(district,year,vdsid,docdf){

  ## here I assume that docdf is a datafame

  ## push 10000 at a time
  i <- 50000
  maxi <- length(docdf[,1])
  if(i > maxi ) i <- maxi

  j <- 1

  db <- couch.makedbname(c(district,year,vdsid))

  couch.makedb(c(district,year,vdsid))

  ## the bulk docs target
  uri=paste(couchdb,db,'_bulk_docs',sep="/")
  reader = nullTextGatherer()
  h = getCurlHandle()

  while(length(docdf)>0) {

    chunk <- docdf[j:i,]
    if( i == length(docdf[,1]) ){
      docdf <- data.frame()
    }else{
      docdf <- docdf[-j:-i,]
    }
    ## for next iteration
    if(length(docdf) && i > length(docdf[,1])) i <- length(docdf[,1])
    bulkdocs <- jsondump4(chunk)
    curlPerform(
                url = uri
                ,httpheader = c('Content-Type'='application/json')
                ,customrequest = "POST"
                ,postfields = bulkdocs
                ,writefunction = reader$update
                ,curl = h
                )

    rm(jsondocs,chunk)

  }

  gc()

}
## testing two different ways


## jsondump1 <- function(chunk){
##   jsondocs <- list()
##   for( row in 1:length(chunk[,1]) ){
##     keepcols <- !is.na(chunk[row,])
##     jsondocs[row] <- toJSON(chunk[row,keepcols])
##   }
##   bulkdocs = paste('{"docs":[',paste(jsondocs, collapse=','),']}',sep='')
##   ## fix JSON:  too many spaces, NA handled wrong
##   bulkdocs <- gsub("\\s\\s*"," ",x=bulkdocs,perl=TRUE)
##   ## this next isnot needed now that I am stripping NA entries above, but better safe
##   bulkdocs <- gsub(" NA"," null"  ,x=bulkdocs  ,perl=TRUE)
##   bulkdocs
## }


## jsondump2 <- function(chunk){
##   bulkdocs <- paste('{"docs":[',paste( ddply(chunk,"ts",toJSON)$V1,collapse=','),']}')
##   ## fix JSON:  too many spaces, NA handled wrong
##   bulkdocs <- gsub("\\s\\s*"," ",x=bulkdocs,perl=TRUE)
##   ## this next is needed again
##   bulkdocs <- gsub(" NA"," null"  ,x=bulkdocs  ,perl=TRUE)
##   bulkdocs
## }


## jsondump3 <- function(chunk){
##   bulkdocs <- paste('{"docs":[',
##     foreach(i=1:nrow(chunk), .combine=paste) %do%
##     toJSON(chunk[i,])
##                     ,']}')
##   ## fix JSON:  too many spaces, NA handled wrong
##   bulkdocs <- gsub("\\s\\s*"," ",x=bulkdocs,perl=TRUE)
##   ## this next is needed again
##   bulkdocs <- gsub(" NA"," null"  ,x=bulkdocs  ,perl=TRUE)
##   bulkdocs
## }

## and the winner is:
jsondump4 <- function(chunk){
  colnames <- names(chunk)

  text.cols    <-  grep( pattern="^(_id|ts)$",x=colnames,perl=TRUE)
  numeric.cols <-  grep( pattern="^(_id|ts)$",x=colnames,perl=TRUE,invert=TRUE)

  ## numeric.cols <- 1:35
  ## text.cols <- 36:37

  num.data <- apply(chunk[,numeric.cols],1,toJSON)
  text.data <- apply(chunk[,text.cols],1,toJSON)
  bulkdocs <- gsub('} {',',',x=paste(num.data,text.data,collapse=','),perl=TRUE)
  bulkdocs <- paste('{"docs":[',bulkdocs,']}')
  ## fix JSON:  too many spaces, NA handled wrong
  bulkdocs <- gsub("\\s\\s*"," ",x=bulkdocs,perl=TRUE)
  ## this next is needed again
  bulkdocs <- gsub("[^,{}:]*:\\s*NA\\s*,"," "  ,x=bulkdocs  ,perl=TRUE)
  bulkdocs
}


