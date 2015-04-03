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
##' Added the idea of splitting up the columns more formally.  I was
##' dead-reckoning before based on knowledge of the input set; now
##' there are parameters.  And in the parent call I run actual code to
##' identify is.character, is.numeric and other on the columns.
##'
##' If you do not supply the number, text columns, I'll do the test
##' locally.  Just not so efficient if you're dumping lots and lots of
##' data to keep re-running it
##'
##' As an example, if you pass in:
##'
##'   x y                  ts                      id      vol         occ
##'   1 1 2012-01-01 00:00:00 1_1_2012-01-01 00:00:00 218.5119 0.406479106
##'   1 1 2012-01-01 01:00:00 1_1_2012-01-01 01:00:00 212.3248 0.180804537
##'   1 1 2012-01-01 02:00:00 1_1_2012-01-01 02:00:00 184.5133 0.009357087
##'   1 1 2012-01-01 03:00:00 1_1_2012-01-01 03:00:00 190.6849 0.507734750
##'   1 1 2012-01-01 04:00:00 1_1_2012-01-01 04:00:00 203.3100 0.337674281
##'   1 1 2012-01-01 05:00:00 1_1_2012-01-01 05:00:00 192.3677 0.671313804
##'
##'
##' you will get out:
##'
##' {"x":1,"y":1,"vol":218.511945384685,"occ":0.406479105586186,"id":"1_1_2012-01-01 00:00:00","ts":1325404800},
##' {"x":1,"y":1,"vol":212.32483922734,"occ":0.180804537376389,"id":"1_1_2012-01-01 01:00:00","ts":1325408400},
##' {"x":1,"y":1,"vol":184.513335562672,"occ":0.00935708731412888,"id":"1_1_2012-01-01 02:00:00","ts":1325412000},
##' {"x":1,"y":1,"vol":190.684859753449,"occ":0.507734750164673,"id":"1_1_2012-01-01 03:00:00","ts":1325415600},
##' {"x":1,"y":1,"vol":203.310037103563,"occ":0.337674280628562,"id":"1_1_2012-01-01 04:00:00","ts":1325419200},
##' {"x":1,"y":1,"vol":192.367654353324,"occ":0.671313803875819,"id":"1_1_2012-01-01 05:00:00","ts":1325422800}
##'
##' Notice the number columns 'x', 'y', 'vol', and 'occ' are kept as
##' numbers, not text, while "id" is converted to text.  The time
##' column 'ts' is neither numeric nor character, so it gets processed
##' independently, and ends up being converted to seconds since the
##' epoch.  If you don't want that, then be careful coming in (like
##' specify that it is character, or be careful about the
##' representation of timezone, etc)
##'
##' @title jsondump6
##' @param chunk an R thing
##' @param bulk TRUE.  Not sure what else it might be, but if it is
##' true then you get bulk doc semantics slapped to the front of the
##' returned JSON string. If FALSE, then you don't.
##' @param num.cols a list of number columns in chunk (either
##' positions or names, going to subset by them as in
##' chunk[,num.cols])
##' @param txt.cols a list of text columns
##' @param oth.cols a list of columns that are neither text nor number
##' or that you otherwise want to hold out separate when cranking out
##' JSON
##' @return formatted JSON for submitting to CouchDB
##' @author James E. Marca
jsondump6 <- function(chunk,bulk=TRUE,num.cols,txt.cols,oth.cols){
    colnames <- names(chunk)
    if(missing(num.cols)){
        ## sort columns into numeric and text
        num.cols <-  unlist(plyr::llply(chunk[1,],is.numeric))
        num.cols <- colnames[num.cols]
    }else{
        if(is.numeric(num.cols[1])){
            num.cols <- colnames[num.cols]
        }
    }
    if(missing(txt.cols)){
        txt.cols <- unlist(plyr::llply(chunk[1,],is.character))
        txt.cols <- colnames[txt.cols]
    }else{
        if(is.numeric(txt.cols[1])){
            txt.cols <- colnames[txt.cols]
        }
    }

    oth.cols <- setdiff(x=colnames,y=c(txt.cols,num.cols))

    num.data <- json.chunklet(chunk,num.cols)
    txt.data <- json.chunklet(chunk,txt.cols)
    json_str <- paste(num.data,
                      txt.data,
                      collapse=',')
    if(!is.null(oth.cols) && length(oth.cols)>0){
        oth.data <- json.chunklet(chunk,oth.cols)
        json_str <- paste(num.data,
                          txt.data,
                          oth.data,
                          collapse=',')
    }

    bulkdocs <- gsub('} {',',',x=json_str,perl=TRUE)

    if(bulk){  bulkdocs <- paste('{"docs":[',bulkdocs,']}') }
    ## fix JSON:  too many spaces, NA handled wrong
    ##  bulkdocs <- gsub("\\s+"," ",x=bulkdocs,perl=TRUE)

    ## this next is needed because NA is not valid JSON
    ## bulkdocs <- gsub("[,?{}:]*:\\s*NA\\s*,"," "  ,x=bulkdocs  ,perl=TRUE)

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
            if(is.na(chunk[i,names])){
                jsonarray <- c(jsonarray,'{}')
            }else{
                jsonarray <- c(jsonarray,
                               paste('{"',names,'":',
                                     rjson::toJSON(chunk[i,names]),
                                     '}',
                                     sep='')
                               )
            }
        }
        res <- jsonarray
    }else{
        res <- apply(chunk[,names],1,rjson::toJSON)
        res <- gsub('"[^\\"]*?":"NA",?',"",x=res,perl=TRUE)
        res <- gsub(',}',"}",x=res,perl=TRUE)
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
