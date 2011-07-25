library('RCurl')
library('RJSONIO')
library('zoo')

source('./generate27Variables.R')
dbname = "vdsdata"
## set up some global setting from environment variables
## defaults
STEPSIZE <- 4
CLEAN.ON.START=TRUE
## check the environment
iwantthese = c("PEMS_STEP_SIZE","CLEAN_ON_START")
envvars <- Sys.getenv(iwantthese)
fieldcheck <- iwantthese %in% names(envvars)

if(fieldcheck[1] && envvars[[1]] != '' ){
 STEPSIZE <- as.numeric(envvars[[1]])
}
if(fieldcheck[2] && envvars[[2]] != ''){
 CLEAN.ON.START=envvars[[2]]
}

source('./couchUtils.R')

filemap <- data.frame('tmpfile'='notarealfile','file'='notarealfile',stringsAsFactors=FALSE)

map.temp.files <- function(file,tmpfile=NULL){

  result <- c()
  if( length(tmpfile) == 0 ){
    result = filemap$tmpfile[filemap$file==file]
  }else{
    print('storage case')
    filemap <<- rbind(filemap,list('tmpfile'=tmpfile,'file'=file))
    result <- filemap$tmpfile[filemap$file==file]
  }
  result

}

unmap.temp.files <- function(file){

  tmp <- map.temp.files(file)
  filemap <<- filemap[filemap$file==file,]
  unlink(tmp)

}

load.remote.file <- function(server,service,root,file){
  tmp <- map.temp.files(file)
  if(length(tmp)==0){
    tmp <- tempfile('remotedata')
    uri <- paste(server,service,root,sep='/')
    system2('curl',paste('--retry 4 ',uri,file,sep=''),stdout=tmp,stderr=FALSE)
    print(map.temp.files(file,tmp))
  }
  load.result <-  load(file=tmp)
  df
}



get.filenames <- function(server='http://localhost:3000'
                          ,service='vdsdata'
                          ,base.dir='D12'
                          ,pattern="RData$"){
  uri=paste(server,service,base.dir,sep="/")
  uri = paste(uri,paste('pattern=',pattern,sep=''),sep='?')
  print(uri)
  reader = basicTextGatherer()
  curlPerform(
              url = uri
              ,writefunction = reader$update
              )
  unlist(fromJSON(reader$value()))
}



couch.daily.doc.save <- function(district,year,vdsid,docdf,local=TRUE){

  colnames <- names(docdf)
  keepnames <-  grep( pattern="^ymd",x=colnames,perl=TRUE,invert=TRUE )

  ## save day as one document per day using split
  docs <- split(docdf,docdf$ymd)

  db <- couch.makedbname(c(district,year,vdsid))

  for(day in 1:length(docs)){
    ## document id is constructed as vdsid ymd mintime.  for example "1209304 2007-01-01 00:00:00"
    doc <- docs[[day]]
    mints <- doc$ymdtime[1]
    id <- paste(vdsid,format(mints,"%Y-%m-%d %H:%M:%S"))
    couch.put(db,id,doc[,keepnames],local=local,dumper=jsondump.data)
  }
  gc()

}

## override the version in couchUtils.R
jsondump.data <- function(chunk){
  jsonchunk <- toJSON(chunk)
  bulkdocs <- gsub('} {',',',x=paste(jsonchunk,collapse=','),perl=TRUE)
  bulkdocs <- paste('{"data":',bulkdocs,'}')
  ## fix JSON:  too many spaces, NA handled wrong
  bulkdocs <- gsub("\\s+"," ",x=bulkdocs,perl=TRUE)
  ## this next is needed again
  bulkdocs <- gsub("[^,{}:]*:\\s*NA\\s*,"," "  ,x=bulkdocs  ,perl=TRUE)
  bulkdocs <- gsub("\\s+NA","null"  ,x=bulkdocs  ,perl=TRUE)
  bulkdocs
}


process.pattern.files <- function(year,file.server="http://lysithia.its.uci.edu:3000",service='vdsdata',base.dir="D12",reconsider=FALSE,pattern=paste("ML_",year,".df.*RData$",sep='')){

  district <- grep( pattern="^D\\d\\d",x=unlist(strsplit(base.dir,split="/")),perl=TRUE,value=TRUE)
  district <- tolower(district)
  ## make sure tracking db is created
  ## couch.makedb(c(district,year))
  ## couch.makedb(c(district,year),local=FALSE)
  ## commented out because it is created and I am sick of the error messages

  ## # set up replication from remote to local and vice versa
  ## replicateid <- couch.makedbname(c(district,year))
  ## local <- couch.makedbname.noescape(c(district,year))
  ## remote <- paste(couchdb,couch.makedbname(c(district,year)),sep="/")
  ## print(paste('setting replication between ',remote,'and', local,sep='  '))
  ## print(couch.start.replication(remote,local,paste(replicateid,'_pull',sep=''),continuous=TRUE))
  ## print(couch.start.replication(local,remote,paste(replicateid,'_push',sep=''),continuous=TRUE))

  files <- get.filenames(file.server,service,base.dir,pattern)

  todo <- length(files)
  print ( paste ( "processing",todo,"files") )

  for( f in files){
    file.names <- strsplit(c(base.dir,f),split="/")
    file.names <- unlist(file.names)
    ## trip the empties
    file.names <- file.names[file.names!='']
    path <- paste(file.names[1:(length(file.names)-1)],collapse="/")
    fname <-  strsplit(file.names[length(file.names)],"\\.")[[1]][1]
    district <- grep( pattern="^D\\d\\d$",x=file.names,perl=TRUE,value=TRUE)
    district <- tolower(district)

    vds.id <-  substr(fname,1,attr(regexpr(pattern="^(\\d{6,7})",fname,perl=TRUE),"match.length"))

    h=getCurlHandle()
    ## storing state in couchdb
    if(reconsider || ! couch.check.is.processed(district,year,vds.id,deldb=CLEAN.ON.START,h=h) ){
      print (paste('processing',vds.id,year))

      ## make sure the filename is in the tracking db, so that it is
      ## easier to figure stuff out later
      couch.save.is.processed(district,year,vds.id,doc=list('file'=paste(base.dir,f,sep='')),h=h)
      if(reconsider){
        couch.deletedb(c(district,year,vds.id))
        ## also make sure replication target is not there
        couch.deletedb(c(district,year,vds.id),local=FALSE)
      }
      couch.makedb(c(district,year,vds.id))
      ## prevent js fixing code from "fixing"
      db <- couch.makedbname(c(district,year,vds.id))
      couch.put(db,'redo',data.frame('redo'=1),h=h)
      couch.put(db,'shrink',data.frame('shrink'=1),h=h)
      rm(h)
      ## split into 4 steps, to reduce total RAM hit per processor core
      for( step in 1:STEPSIZE ){

        df <- load.remote.file(file.server,service,base.dir,f)
        rows <- nrow(df)
        quarter <- ceiling(rows/STEPSIZE)
        start.idx <- (step - 1)*quarter
        end.idx <- (step)*quarter
        if(rows < 10000){
          start.idx <- 0
          end.idx <- rows
          step <- STEPSIZE
        }
        ## adjust to eliminate the 40 row aliasing from each process
        if(step == 1){
          ## first step, adjust the end point
          end.idx <- end.idx + 40
        }else{
          start.idx <- start.idx - 40
        }

        if(end.idx > rows) end.idx <- rows
        if(start.idx < 1) start.idx <- 1

        if(step == STEPSIZE){
          ## done with temp file
          unmap.temp.files(f)
        }

        df <- df[start.idx:end.idx,]

        ## continue as before
        varnames <- names(df)
        stats <- data.frame()
        statresult <- try( stats <- generate27variables(df) )
        rm (df)
        ## I keep getting errors saving, that bomb out my program.
        if(class(statresult) == "try-error"){
          print ("\n Error computing stats \n")
          couch.save.is.processed(district,year,vds.id,doc=list(err='stats computation error'))
        }else{
          colnames <- names(stats)
          text.cols    <-  grep( pattern="^(_id|ts)$",x=colnames,perl=TRUE)
          numeric.cols <-  grep( pattern="^(_id|ts)$",x=colnames,perl=TRUE,invert=TRUE)

          #  keeprows <- apply(stats[numeric.cols],1,function(x){! all(is.na(x))})
          #  stats <- stats[keeprows,]

          ## stats['_id'] <- paste(vds.id,stats$ts)
          stats$ymdtime <- stats$ts
          stats$ymd <- format(stats$ts,"%Y-%m-%d") ## for daily grouping
          stats$ts <- format(stats$ts,"%Y-%m-%d %H:%M:%S %Z") ## for toJSON to handle better
          ## initial clean up
          ## problems, handle later
          if(exists('error',stats)){
            couch.save.is.processed(district,year,vds.id,doc=list(err=stats$err))
          }else{

            print('saving results')
            ## save the documents, one per record
            couch.daily.doc.save(district,year,vds.id,stats)

          }
          rm(stats)
        }
        ## all done, save that state
        couch.save.is.processed(district,year,vds.id, list(processed=step/STEPSIZE))
        gc()
        ##if(step == STEPSIZE){
          print ('replicate to remote db')
          ## now that local save is done, must replicate to remote
          src <- couch.makedbname.noescape(c(district,year,vds.id))
          tgt <- paste(privcouchdb,couch.makedbname(c(district,year,vds.id)),sep="/");
          couch.start.replication(src,tgt)
        ##}

        if(rows < 10000){
          couch.save.is.processed(district,year,vds.id, list(processed=1))
          break
        }
      }
    }else{
      print (paste(vds.id,year,'already done'))
    }
  }
}

file.server <- "http://lysithia.its.uci.edu:3000"
vdsservice <- "vdsdata"
## basedir <- paste("D12",c(22,55,5,57,73,91,241,133,605,261,405),sep='/')
years <- c(2007,2008,2009)

year <- years[1]
process.pattern.files(year,file.server,vdsservice,reconsider=FALSE)


## patterns <- paste(todo.vdsids,"_ML_",year,".df.*RData$",sep='')
## for(pattern in patterns){
##   process.pattern.files(year,file.server,vdsservice,reconsider=FALSE,pattern=pattern)
## }
