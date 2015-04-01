
##################################################
## revised to use multi-year, multi-district tracking db
couch.check.state <- function(year,vdsid,process, local=TRUE,db=trackingdb){
  statusdoc <- couch.get(db,vdsid,local=local)
  result <- 'error' ## default to error
  current.names <- names(statusdoc)
  if('error' %in% current.names && length(names)==2){
    result <- 'todo'
  }else{
    fieldcheck <- c(process) %in% names(statusdoc[[paste(year)]])
    if(!fieldcheck[1] ){
      ## either no status doc, or no recorded state for this process, mark as 'todo'
      result <- 'todo'
    }else{
      result <- statusdoc[[paste(year)]][[process]]
    }
  }
  result
}

couch.checkout.for.processing <- function(district,year,vdsid,
                                          process,state='inprocess',
                                          local=TRUE, force=FALSE,
                                          db=trackingdb){
  result <- 'done' ## default to done
  statusdoc = couch.get(db,vdsid,local=local)
  fieldcheck <- c('error',process) %in% names(statusdoc[[paste(year)]])
  if( (fieldcheck[1] && statusdoc['error'] == "not_found") ){
    result = 'todo'
    statusdoc = list() ## R doesn't interpolate variables in statements like list(process='state')
    statusdoc[paste(year)]=list()
    statusdoc[[paste(year)]][[process]]=state
    putstatus <- fromJSON(couch.put(db,vdsid,statusdoc,local=local))
    fieldcheck <- c('error') %in% names(putstatus)
    if(fieldcheck[1]){
      result <- 'error'
    }

  }else if( !fieldcheck[2] ||  statusdoc[[paste(year)]][[process]] == 'todo' || force ){
    result = 'todo'
    statusdoc[[paste(year)]][[process]]=state
    putstatus <- fromJSON(couch.put(db,vdsid,statusdoc,local=local))
    fieldcheck <- c('error') %in% names(putstatus)
    if(fieldcheck[1]){
      result <- 'error'
    }
  }else{
    ## have status doc, process field, but not in 'todo' state, so report what state it is in
    result = statusdoc[[paste(year)]][[process]]
  }
  result
}

couch.set.state <- function(year,detector.id, doc,
                            local=TRUE, h=getCurlHandle(),
                            db=trackingdb){

  current = couch.get(db,detector.id,local=local,h=h)
  doc.names  <- names(doc)
  current.names <- names(current)
  if('error' %in% current.names && length(current.names) == 2){
    ## error means there isn't a current document in the db
    current = list() ## R doesn't interpolate variables in statements like list(process='state')
    current[[paste(year)]][doc.names] = doc
  }else{
    ## have some data in the tracking db for this doc
    ## just append/overwrite the state doc information for the given year
    current[[paste(year)]][doc.names] = doc
  }
  ## clean mess
  if('error' %in% current.names && length(current.names) > 2){
    current[['error']] <- NULL
    current[['reason']] <- NULL
  }
  couch.put(db,detector.id,current,local=local,h=h)

}

#########

## testing different ways to dump JSON


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


##' A custom JSON dumper that fixes some of the irritants of RJSONIO::toJSON
##'
##' @title jsondump4old
##' @param chunk an R thing
##' @return formatted JSON for submitting to CouchDB
##' @author James E. Marca
jsondump4old <- function(chunk){
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
  bulkdocs <- gsub("\\s+"," ",x=bulkdocs,perl=TRUE)
  ## this next is needed again
  bulkdocs <- gsub("[^,{}:]*:\\s*NA\\s*,"," "  ,x=bulkdocs  ,perl=TRUE)
  bulkdocs
}

##' A custom JSON dumper that fixes some of the irritants of RJSONIO::toJSON
##'
##' @title jsondump4
##' @param chunk an R thing
##' @return formatted JSON for submitting to CouchDB
##' @author James E. Marca
jsondump4 <- function(chunk,bulk=TRUE){
  colnames <- names(chunk)

  text.cols    <-  grep( pattern="^(_id|ts)$",x=colnames,perl=TRUE)
  numeric.cols <-  grep( pattern="^(_id|ts)$",x=colnames,perl=TRUE,invert=TRUE)

  ## numeric.cols <- 1:35
  ## text.cols <- 36:37

  num.data <- apply(chunk[,numeric.cols],1,toJSON)
  text.data <- list()
  if(length(text.cols) < 2){
    text.data <- toJSON(chunk[,text.cols])
  }else{
    text.data <- apply(chunk[,text.cols],1,toJSON)
  }
  bulkdocs <- gsub('} {',',',x=paste(num.data,text.data,collapse=','),perl=TRUE)
  if(bulk){  bulkdocs <- paste('{"docs":[',bulkdocs,']}') }
  ## fix JSON:  too many spaces, NA handled wrong
  bulkdocs <- gsub("\\s+"," ",x=bulkdocs,perl=TRUE)
  ## this next is needed again
  bulkdocs <- gsub("[^,{}:]*:\\s*NA\\s*,"," "  ,x=bulkdocs  ,perl=TRUE)
  bulkdocs
}

##' A custom JSON dumper that fixes some of the irritants of RJSONIO::toJSON
##'
##' @title jsondump5
##' @param chunk an R thing
##' @return formatted JSON for submitting to CouchDB
##' @author James E. Marca
jsondump5 <- function(chunk){
  jsonchunk <- RJSONIO::toJSON(chunk)
  bulkdocs <- gsub('} {',',',x=paste(jsonchunk,collapse=','),perl=TRUE)
  ## fix JSON:  too many spaces, NA handled wrong
  bulkdocs <- gsub("\\s+"," ",x=bulkdocs,perl=TRUE)
  ## this next is needed again
  bulkdocs <- gsub("[^,{}:]*:\\s*NA\\s*,"," "  ,x=bulkdocs  ,perl=TRUE)
  bulkdocs <- gsub("\\s+NA","null"  ,x=bulkdocs  ,perl=TRUE)
  bulkdocs
}


## PUT somedatabase/document/attachment?rev=123 HTTP/1.0
## Content-Length: 245
## Content-Type: image/jpeg
## <JPEG data>
##################################################
## PUT /test_suite_db/multipart HTTP/1.1
## Content-Type: multipart/related;boundary="abc123"

## --abc123
## content-type: application/json

## {"body":"This is a body.",
## "_attachments":{
##   "foo.txt": {
##     "follows":true,
##     "content_type":"text/plain",
##     "length":21
##     },
##   "bar.txt": {
##     "follows":true,
##     "content_type":"text/plain",
##     "length":20
##     },
##   }
## }

## --abc123

## this is 21 chars long
## --abc123

## this is 20 chars lon
## --abc123--

couch.attach <- function(db=trackingdb,docname,attfile, local=TRUE, priv=FALSE, h=getCurlHandle()){

  current = couch.get(db,docname,local=local,h=h)
  revision <- paste('rev',current['_rev'],sep='=')

  cdb <- localcouchdb
  if(!local){
    cdb <- couchdb
  }

  file.path <- unlist(strsplit(attfile,"/"))
  flen <- length(file.path)
  filename <- file.path[flen]

  uri=paste(cdb,db,docname,filename,sep="/");
  uri=gsub("\\s","%20",x=uri,perl=TRUE)
  uri <- paste(uri,revision,sep='?')
  if(priv){
    couch.session(h,local)
  }

  content.type <- guessMIMEType(attfile, "application/x-binary")

  print(paste('putting attachment'))
  putting.command <- paste('curl',paste('-v -X PUT -H "Content-Type: ',content.type,'" ',uri,' --data-binary @',attfile,sep=''))
  ## have to wait, in case there are other docs to attach
  ## until I figure out how to multiple at a time deal thingee
    r <- try(
             print(system2('curl',paste('-v -X PUT -H "Content-Type: ',content.type,'" ',uri,' --data-binary @',attfile,sep=''),wait=TRUE ,stdout=TRUE,stderr=TRUE))
             )
    if(class(r) == "try-error") {
      print('doit later')
      ## make a note of it
      cat(paste('couch.attach failed:',putting.command,'\n' ),file='failedcurl.log',append=TRUE)
    }else{
      print('success')
    }
}

couch.get.attachment <- function(db=trackingdb,docname,attachment, local=TRUE){##, h=getCurlHandle()){
  cdb <- localcouchdb
  if(!local){
    cdb <- couchdb
  }
  uri=paste(cdb,db,docname,attachment,sep="/");
  uri=gsub("\\s","%20",x=uri,perl=TRUE)
  tmp <- tempfile(paste('remotedata',attachment,sep='_'))
  file.create(tmp)
  print(paste('getting attachment',uri))
  r <- try(
       system2('curl',paste('-s -S -o',tmp,uri),stdout=FALSE,stderr=FALSE,wait=TRUE)
    )
  if(class(r) == "try-error"){
    ## try one more time
    r <- try(
       system2('curl',paste('-v -o',tmp,uri),stdout=FALSE,stderr=FALSE,wait=TRUE)
      )
    if(class(r) == "try-error"){
      ## give up
      print('curl failed after two tries to download attachment')
    }else{
      print('success downloading, second try')
    }
  }else{
    print('success downloading, first try')
  }
  ## load it here
  res <- load(tmp, .GlobalEnv)
  unlink(tmp)
  res
}

couch.has.attachment <- function(db=trackingdb,docname,attachment,local=TRUE){
  r <- couch.get(db,docname)
  attachments <- r[['_attachments']]
  attachment %in% names(attachments)
}
