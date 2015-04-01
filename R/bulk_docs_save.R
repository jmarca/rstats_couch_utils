##' null reader for RCurl when bulk saving
##'
##' pretty much copied from the RCurl docs, as I recall
##'
##' This is a function that is used to create a closure (i.e. a
##' function with its own local variables whose values persist across
##' invocations).  This is called to provide an instance of a function
##' that is called when the libcurl engine has some text to be
##' processed as it is reading the HTTP response from the server.  The
##' function that reads the text can do whatever it wants with
##' it. This one simply cumulates it and makes it available via a
##' second function.
##'
##' @param txt
##' @param max
##' @param value
##' @return a null text gatherer function for RCurl invocation
##' @author James E. Marca
nullTextGatherer <- function(txt = character(), max = NA, value = NULL)
{
  update = function(str) {
    ## let the prior string spill onto floor
    txt <<-   c(txt)
    nchar(str, "bytes") # use bytes rather than chars as for UTF-8,
                        # etc. we may have fewer characters, but the C
                        # code for libcurl works in bytes. If we
                        # report chars and < bytes, libcurl terminates
                        # the download.
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

##' couch bulk docs saver
##'
##' save more than one doc at a time.  In fact, by default save 1000 at a time.
##'
##' @title couch.bulk.docs.save
##' @param db the database to save into.  Default to whatever is in
##' the config file
##' @param docdf the document to save, as a dataframe
##' @param local cruft, ignore, but kept for backwards compatibility
##' @param chunksize defaults to 1000.  How many docs to write at a time
##' @param makeJSON a function to use to create JSON
##' @return 1, or die trying
##' @export
##' @author James E. Marca
couch.bulk.docs.save <- function(db='default',
                                 docdf,
                                 local=TRUE,
                                 chunksize=1000,
                                 makeJSON=jsondump4){
    ## here I assume that docdf is a datafame

    config <- read.config()

    if(db == 'default'){
        db <- config$trackingdb
    }

    ## push 1000 at a time
    i <- chunksize
    maxi <- length(docdf[,1])
    if(i > maxi ) i <- maxi

    j <- 1


    ## the bulk docs target
    uri=paste(config$host,':',config$port,'/',db,'/','_bulk_docs',sep='')

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
        bulkdocs <- makeJSON(chunk)
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
    1
}


##' couch bulk docs saver for district, year, vdsid
##'
##' wraps a call to couch.bulk.docs.save.  Creates the right db name
##' from the passed district, year, and vdsid
##'
##' @param district
##' @param year
##' @param vdsid
##' @param docdf the document to save, as a dataframe
##' @param local cruft, ignore, but kept for backwards compatibility
##' @param chunksize defaults to 1000.  How many docs to write at a time
##' @param db the database to save into.  Default to whatever is in
##' the config file
##' @param makeJSON a function to use to create JSON
##' @return 1, or die trying
##' @export
##' @author James E. Marca
couch.async.bulk.docs.save <- function(district,
                                       year,
                                       vdsid,
                                       docdf,
                                       local=TRUE,
                                       chunksize=1000){

    db <- couch.makedbname(c(district,year,vdsid))
    couch.bulk.docs.save(db=db,docdf=docdf,local=local,chunksize=chunksize)
}
