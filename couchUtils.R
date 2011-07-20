## requires that dbname, etc be set externally
## non-local is where to go to be sure (tracking db)
## and also the target of all replication calls
## local is where to send bulk saves,
## and the source of replication calls
couchenv = Sys.getenv(c(
  "COUCHDB_HOST", "COUCHDB_USER", "COUCHDB_PASS", "COUCHDB_PORT"
  , "COUCHDB_LOCALHOST", "COUCHDB_LOCALUSER", "COUCHDB_LOCALPASS", "COUCHDB_LOCALPORT"))

couchdb = paste("http://",couchenv[1],":",couchenv[4],sep='')
privcouchdb = paste("http://",couchenv[2],":",couchenv[3],"@",couchenv[1],":",couchenv[4],sep='')
localcouchdb = paste("http://",couchenv[5],":",couchenv[8],sep='')
localprivcouchdb = paste("http://",couchenv[6],":",couchenv[7],"@",couchenv[5],":",couchenv[8],sep='')

## global curl opts

 options(RCurlOptions = list(
           fresh.connect = TRUE
           ))


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

couch.makedbname.noescape <- function( components ){
  if(!is.null(dbname)){
    components <- c(dbname,components)
  }
  tolower(paste(components,collapse='/'))
}


couch.makedb <- function( db, local=TRUE ){

  if(length(db)>0){
    db <- couch.makedbname(db)
  }
  # print(paste('making db',db))
  uri=paste(privcouchdb,db,sep="/");
  if(local) uri=paste(localprivcouchdb,db,sep="/");
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

couch.deletedb <- function(db, local=TRUE){

  if(length(db)>0){
    db <- couch.makedbname(db)
  }

  privcouchdb = paste("http://",couchenv[2],":",couchenv[3],"@",couchenv[1],":",couchenv[4],sep='')
  uri=paste(privcouchdb,db,sep="/");
  if(local) uri=paste(localprivcouchdb,db,sep="/");

  reader = basicTextGatherer()
  curlPerform(
              url = uri
              ,httpheader = c('Content-Type'='application/json')
              ,customrequest = "DELETE"
              ,writefunction = reader$update
              )

  print(paste('deleted db',db,reader$value(),collapse=' '))
}

couch.post <- function(db,doc,local=TRUE,h=getCurlHandle()){
  cdb <- localcouchdb
  if(!local){
    cdb <- couchdb
  }
  reader = basicTextGatherer()
  curlPerform(
              url = paste(cdb,db,sep="/")
              ,customrequest = "POST"
              ,httpheader = c('Content-Type'='application/json')
              ,postfields = toJSON(doc,collapse='')
              ,writefunction = reader$update
              ,curl=h
              )
  reader$value()

}

couch.get <- function(db,docname, local=TRUE, h=getCurlHandle()){

  if(length(db)>1){
    db <- couch.makedbname(db)
  }
  uri=paste(couchdb,db,docname,sep="/");
  if(local) uri=paste(localcouchdb,db,docname,sep="/");
  fromJSON(getURL(uri)[[1]],curl=h)

}

couch.put <- function(db,docname,doc, local=TRUE, priv=FALSE, h=getCurlHandle(),dumper=jsondump5){

  if(length(db)>1){
    db <- couch.makedbname(db)
  }
  cdb <- localcouchdb
  if(!local){
    cdb <- couchdb
  }
  uri=paste(cdb,db,docname,sep="/");
  uri=gsub("\\s","%20",x=uri,perl=TRUE)
  if(priv){
    couch.session(h)
  }

  reader = basicTextGatherer()

  print(paste('putting',uri))
  curlPerform(
              url = uri
              ,customrequest = "PUT"
              ,httpheader = c('Content-Type'='application/json')
              ,postfields = dumper(doc)
              ,writefunction = reader$update
              ,curl=h
              )
  reader$value()
}

couch.delete <- function(db,docname,doc, local=TRUE){

  if(length(db)>0){
    db <- couch.makedbname(db)
  }

  uri=paste(couchdb,db,docname,sep="/");
  if(local) uri=paste(localcouchdb,db,docname,sep="/");
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

## session isn't a json post, so has its own call to curlPerform
couch.session <- function(h,local=TRUE){
  curlSetOpt(cookiejar='.cookies.txt', curl=h)
  cdb <- couchdb
  name <-couchenv[2]
  pwd <- couchenv[3]
  if(local){
    cdb <- localcouchdb
    name <-couchenv[6]
    pwd <- couchenv[7]
  }
  reader = basicTextGatherer()
  curlPerform(
              url = paste(cdb,"_session",sep="/")
              ,customrequest = "POST"
              ,writefunction = reader$update
              ,postfields = paste(paste('name',name,sep='='),paste('password',pwd,sep='='),sep='&')
              ,curl=h
              ,httpauth='ANY'
              )
  reader$value()
}



couch.check.is.processed <- function(district,year,vdsid,deldb=TRUE, local=TRUE,  h=getCurlHandle()){

  statusdoc = couch.get(c(district,year),vdsid,local=local,h=h)
  result <- TRUE ## default to done
  fieldcheck <- c('error','inprocess','processed') %in% names(statusdoc)
  ## print(fieldcheck)
  if(fieldcheck[1] || ( !fieldcheck[2] && !fieldcheck[3]) ){
    putstatus <- couch.save.is.processed(district,year,vdsid,doc=list('inprocess'=1),h=h)
    # print(putstatus)
    putstatus <- fromJSON(putstatus)
      ##fromJSON(couch.put(c(district,year),vdsid,list('inprocess'=1)))
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


couch.save.is.processed <- function(district,year,vdsid,doc=list(processed=1), local=TRUE, h=getCurlHandle()){

  current = couch.get(c(district,year),vdsid,h=h)
  curr.names <- names(current)
  # print(current)
  if(length(curr.names)>0 && length(current$error)==0 ) {
    doc.names  <- names(doc)
    keep.names <- setdiff(curr.names,doc.names)
    ##    print(paste('current',curr.names,'doc',doc.names,'keep',keep.names))
    if(length(keep.names)>0) doc[keep.names] = current[keep.names]
  }
  ## doc = merge (doc,current)
  couch.put(c(district,year),vdsid,doc,h=h)

}

##################################################
## generalization of the above
##################################################
couch.check.state <- function(district,year,vdsid,process, local=TRUE){
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

couch.checkout.for.processing <- function(district,year,vdsid,process, local=TRUE){
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


## not really async, but whatever.  more like split up into pieces
couch.async.bulk.docs.save <- function(district,year,vdsid,docdf, local=TRUE){

  ## here I assume that docdf is a datafame

  ## push 10000 at a time
  i <- 1000
  maxi <- length(docdf[,1])
  if(i > maxi ) i <- maxi

  j <- 1

  db <- couch.makedbname(c(district,year,vdsid))

  couch.makedb(c(district,year,vdsid))

  ## the bulk docs target
  uri=paste(couchdb,db,'_bulk_docs',sep="/")
  if(local) uri=paste(localcouchdb,db,'_bulk_docs',sep="/");
  reader = nullTextGatherer()

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
    h = getCurlHandle()
    curlresult <- try( curlPerform(
                                   url = uri
                                   ,httpheader = c('Content-Type'='application/json')
                                   ,customrequest = "POST"
                                   ,postfields = bulkdocs
                                   ,writefunction = reader$update
                                   ,curl = h
                                   )
                      )
    if(class(curlresult) == "try-error"){
      print ("\n Error saving to couchdb, trying again \n")
      rm(h)
      h = getCurlHandle()
      curlPerform(
                  url = uri
                  ,httpheader = c('Content-Type'='application/json')
                  ,customrequest = "POST"
                  ,postfields = bulkdocs
                  ,writefunction = reader$update
                  ,curl = h
                  )
    }

  }
  gc()

}

couch.start.replication <- function(src,tgt,id=NULL,continuous=FALSE){

  h = getCurlHandle()
  couch.session(h)
  doc = list("source" = src,"target" = tgt
        , "create_target" = TRUE
        , "continuous" = continuous
	, "user_ctx" = list( "name"="james", "roles"=list("_admin","") )
        )
  if(!is.null(id)){
    current <- couch.get('_replicator',id,local=TRUE,h=h)
    if(length(current$error) == 0){
      ##doc = merge(doc,current)
      doc['_rev']=current['_rev']
    }
  }
  print(paste("setting up replication doc:\n",toJSON(doc)))
  if(is.null(id)){
    ## no id, use post
    print(
          couch.post('_replicator'
               ,doc
               ,local=TRUE
               ,h=h
               )
        )
  }else{
    print(
          couch.put('_replicator'
              ,id
              ,doc
              ,local=TRUE
              ,h=h
              )
          )
  }
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
  bulkdocs <- gsub("\\s+"," ",x=bulkdocs,perl=TRUE)
  ## this next is needed again
  bulkdocs <- gsub("[^,{}:]*:\\s*NA\\s*,"," "  ,x=bulkdocs  ,perl=TRUE)
  bulkdocs
}

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

jsondump5 <- function(chunk){
  jsonchunk <- toJSON(chunk)
  bulkdocs <- gsub('} {',',',x=paste(jsonchunk,collapse=','),perl=TRUE)
  ## fix JSON:  too many spaces, NA handled wrong
  bulkdocs <- gsub("\\s+"," ",x=bulkdocs,perl=TRUE)
  ## this next is needed again
  bulkdocs <- gsub("[^,{}:]*:\\s*NA\\s*,"," "  ,x=bulkdocs  ,perl=TRUE)
  bulkdocs <- gsub("\\s+NA","null"  ,x=bulkdocs  ,perl=TRUE)
  bulkdocs
}


