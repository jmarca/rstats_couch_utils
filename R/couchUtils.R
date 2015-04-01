


couch.makedb <- function( db, local=TRUE ){

  if(length(db)>1){
    db <- couch.makedbname(db)
  }
  # print(paste('making db',db))
  uri=paste(couchdb,db,sep="/");
  if(local) uri=paste(localcouchdb,db,sep="/");
  reader = basicTextGatherer()

  curlPerform(
      url = uri
     ,httpheader = c('Content-Type'='application/json')
     ,customrequest = "PUT"
     ,writefunction = reader$update
     ,userpwd=couch_userpwd
      )

  print(paste( 'making db',db, reader$value() ))
  reader$value()
}

couch.deletedb <- function(db, local=TRUE){

  if(length(db)>1){
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
  uri <- paste(couchdb,db,docname,sep="/");
  if(local) uri <- paste(localcouchdb,db,docname,sep="/");
  uri <- gsub("\\s","%20",x=uri,perl=TRUE)
  ## stupid idea!
  ## uri <- gsub(":","%3A",x=uri,perl=TRUE)
  ## print(uri)
  fromJSON(getURL(uri,curl=h)[[1]],simplify=FALSE)

}

## # Pointer to your couchbase view base.  This is where you find your
## # own data
## urlBase <- 'http://couchbase.example.com/sfpd'

## # This is your basic GET request -> parsed JSON.
## getData <- function(subpath) {
##   fromJSON(file=paste(urlBase, subpath, sep=''))$rows
## }

## # And this flattens it into a data frame, optionaly naming the
## # columns.
## getFlatData <- function(sub, n=NULL) {
##   b <- plyr::ldply(getData(sub), unlist)
##   if (!is.null(n)) {
##     names(b) <- n
##   }
##   b
## }

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
    couch.session(h,local)
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

  if(length(db)>1){
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
    if(length(grep( pattern="error",x=names(doc),perl=TRUE)) > 0){
      ##doc = merge(doc,current)
      doc['_rev']=current['_rev']
    }
  }
  print(paste("setting up replication doc:\n",toJSON(doc)))
  result='ok'
  if(is.null(id)){
    ## no id, use post
    result <-
          couch.post('_replicator'
               ,doc
               ,local=TRUE
               ,h=h
               )

  }else{
    result <-
          couch.put('_replicator'
              ,id
              ,doc
              ,local=TRUE
              ,h=h
              )

  }
  print (result)
  result
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
