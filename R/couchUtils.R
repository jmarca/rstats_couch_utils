##' Checkout a detector for processing
##'
##' This enables a sort of multi-machine communication of jobs.  In
##' practice it doesn't work so well, so I've moved away from it, but
##' the idea is to "check out" a detector by setting a state of a
##' processing step as "in progress" (or better, some date value so
##' that you can repeat jobs in the future without having to scrub
##' states to zero)
##'
##' So, set a job with a date.  Then at some future date, if you want
##' to redo all the jobs that failed, you can look for "in progress
##' {date}" and if date isn't the current job's date, then check it
##' out, uptick the date, and redo the processing task.
##'
##' @title couch.checkout.for.processing
##' @param district the district
##' @param year the year
##' @param vdsid the detector id
##' @param process the process to set to the given state
##' @param state the state to set the process
##' @param force TRUE to force setting the state, FALSE to skip if
##' already set.
##' @param db the db name, will default to whatever is in the config
##' file as trackingdb
##' @return the result of checking out for processing, which is either
##' 'error' if something went wrong, or whatever state the process is
##' currently in if not in the 'todo' state, or the result of setting
##' the "process" to "state"
##' @author James E. Marca
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


##' A custom JSON dumper that fixes some of the irritants of rjson::toJSON
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

##' A custom JSON dumper that fixes some of the irritants of rjson::toJSON
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

##' A custom JSON dumper that fixes some of the irritants of rjson::toJSON
##'
##' @title jsondump5
##' @param chunk an R thing
##' @return formatted JSON for submitting to CouchDB
##' @author James E. Marca
jsondump5 <- function(chunk){
  jsonchunk <- rjson::toJSON(chunk)
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
