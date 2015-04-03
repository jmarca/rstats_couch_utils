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

  num.data <- apply(chunk[,numeric.cols],1,rjson::toJSON)
  text.data <- apply(chunk[,text.cols],1,rjson::toJSON)
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

  num.data <- apply(chunk[,numeric.cols],1,rjson::toJSON)
  text.data <- list()
  if(length(text.cols) < 2){
    text.data <- rjson::toJSON(chunk[,text.cols])
  }else{
    text.data <- apply(chunk[,text.cols],1,rjson::toJSON)
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

##' A rewrite of JSON dump 4
##'
##' @title jsondump6
##' @param chunk an R thing
##' @param bulk TRUE.  Not sure what else it might be, but if it is
##' true then you get bulk doc semantics slapped to the front of the
##' returned JSON string. If FALSE, then you don't.
##' @return formatted JSON for submitting to CouchDB
##' @author James E. Marca
jsondump6 <- function(chunk,bulk=TRUE,text.cols,numeric.cols){
    colnames <- names(chunk)

    if(missing(text.cols)){
        candiates <- colnames
        if(!missing(numeric.cols)){
            candidates <- setdiff(colnames,numeric.cols)
        }
        text.cols <- grep( pattern="^(_id|ts)$",
                          x=candidates,perl=TRUE,value=TRUE)
    }
    if(missing(numeric.cols)){
        candidates <- setdiff(colnames,text.cols)
        numeric.cols <-  grep( pattern="^(_id|ts)$",
                              x=candidates,
                              perl=TRUE,invert=TRUE,value=TRUE)
    }

    other.cols <- setdiff(x=colnames,y=text.cols)
    other.cols <- setdiff(x=other.cols,y=numeric.cols)

    num.data <- json.chunklet(chunk,numeric.cols)
    txt.data <- json.chunklet(chunk,text.cols)
    oth.data <- json.chunklet(chunk,other.cols)

    bulkdocs <- gsub('} {',',',x=paste(
                                   num.data,
                                   txt.data,
                                   oth.data,
                                   collapse=',')
                    ,perl=TRUE)

    if(bulk){  bulkdocs <- paste('{"docs":[',bulkdocs,']}') }
    ## fix JSON:  too many spaces, NA handled wrong
    ##  bulkdocs <- gsub("\\s+"," ",x=bulkdocs,perl=TRUE)
    ## this next is needed again
    ##  bulkdocs <- gsub("[^,{}:]*:\\s*NA\\s*,"," "  ,x=bulkdocs  ,perl=TRUE)
    bulkdocs
}

##' apply rjson::toJSON to a subset of a dataframe, by rows
##'
##' I hate that stupid JSON tools in R work by column, not by row.  I
##' want docs in couchdb by row.  So I need apply.  This does that
##'
##' The reason this exists is that the toJSON util will convert the
##' input data into a list, and that conversion invariably will smush
##' all the data to the same type.  So I am separating out text and
##' numbers to avoid that, which means I either have lots of copy
##' paste of this code, or a nice tidy function
##'
##' @title json.chunklet
##' @param chunk some part of a dataframe
##' @param names the dimension names that you want to write out as
##' JSON records from the chunk.
##' @return a list of JSON strings, one per row.
##' @author James E. Marca
json.chunklet <- function(chunk,names){
    res <- NULL
    if(length(names) == 1){
        jsonarray <- NULL
        for(i in 1:length(chunk[,names])){
            jsonarray <- c(jsonarray,
                           paste('{"',names,'":',
                                 rjson::toJSON(chunk[i,names]),
                                 '}',
                                 sep='')
                           )
        }
        res <- jsonarray
    }else{
        res <- apply(chunk[,names],1,rjson::toJSON)
    }
    res
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
