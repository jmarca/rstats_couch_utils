##' Get couchdb _all_docs, or any previously defined view.  The
##' CouchDB view and _all_docs APIs are about the same.  This function
##' lets you query either one, and get back all the docs that satisfy
##' your query parameters.
##'
##' It doesn't yet allow the POST versions of _all_docs and view API,
##' so you can't pass in a list of doc ids to retrieve. To do that,
##' you want to use \code{\link{couch.allDocsPost}}
##'
##' @title couch.allDocs
##' @param db the database to query
##' @param query the query parameters, as a named list, named vector
##' @param view the view to query, will default to '_all_docs'
##' @param include.docs TRUE or FALSE, defaults to TRUE.  Whether or
##' not to download the document content, or to just get a list of the
##' doc ids and revisions.  CouchDB offers both choices.  In no case
##' will this function download attachments as well
##' @return the result of the query, parsed into R lists or whatnot
##' @author James E. Marca
##' @export
couch.allDocs <- function(db, query, view='_all_docs',
                          include.docs = TRUE
                          ){

    h=RCurl::getCurlHandle()

    if(length(db)>1){
        db <- couch.makedbname(db)
    }
    couchdb <-  couch.get.url()
    couch_userpwd <- couch.get.authstring()
    uri <- paste(couchdb,db,view,sep="/");
    q <- ''
    if(!missing(query)){
        q <- NULL
        qnames <- names(query)
        for(i in 1:length(qnames)){
            qi <- paste(
                qnames[i],
                RCurl::curlEscape(
                    rjson::toJSON(
                        query[[i]]
                        )
                    ),
                sep='='
                )
            q <- paste(c(q,qi),collapse='&')
        }
    }
    if(include.docs){
        q <- paste(q,'include_docs=true',sep='&')
    }
    uri <- paste(uri,q,sep='?')
    reader <- RCurl::basicTextGatherer()
    if(is.null(couch_userpwd)){
        RCurl::curlPerform(
            url = uri
           ,customrequest = "GET"
           ,httpheader = c('Content-Type'='application/json')
           ,writefunction = reader$update
           ,curl=h
            )
    }else{
        RCurl::curlPerform(
            url = uri
           ,customrequest = "GET"
           ,httpheader = c('Content-Type'='application/json')
           ,writefunction = reader$update
           ,curl=h
           ,userpwd=couch_userpwd
            )
    }
    rjson::fromJSON(reader$value()[[1]])
}



##' Get couchdb _all_docs, or any previously defined view.  This
##' version is the POST API version, so you can pass some list of
##' parameters in and they will be converted into a JSON document and
##' POSTed to the CouchDB server.
##'
##' This version is not a query/GET version. To do that, you want to
##' use the \code{\link{couch.allDocs}} version.
##'
##' To quote the CouchDB docs (as of 1.6.1),
##' \tabular{l}{
##' Unlike GET
##' /{db}/_design/{ddoc}/_view/{view} for accessing views, the POST
##' method supports the specification of explicit keys to be retrieved
##' from the view results. The remainder of the POST view
##' functionality is identical to the GET
##' /{db}/_design/{ddoc}/_view/{view} API.
##' }
##'
##' For this reason, the parameter here is called keys.  You can also
##' pass other query options inside of keys, but that type of usage
##' isn't well supported or tested.
##'
##' @title couch.allDocsPost
##' @param db the database to query
##' @param keys the keys or query parameters, as a named list, named
##' vector.  If keys is not passed in, then this function will
##' actually hand off to \code{\link{couch.allDocs}} to do the work
##' @param view the view to query, will default to '_all_docs'
##' @param include.docs TRUE or FALSE, defaults to TRUE.  Whether or
##' not to download the document content, or to just get a list of the
##' doc ids and revisions.  CouchDB offers both choices.  In no case
##' will this function download attachments as well
##' @param h an RCurl handle, will default to getting anew one.
##' @return the result of the query, parsed into R lists or whatnot
##' @author James E. Marca
couch.allDocsPost <- function(db,
                              keys,
                              view='_all_docs',
                              include.docs = TRUE,
                              h=RCurl::getCurlHandle()){
    ## bounce over to the GET version if keys isn't passed in
    if(missing(keys)){
        return (couch.allDocs(db,view=view,include.docs=include.docs))
    }

    if(length(db)>1){
        db <- couch.makedbname(db)
    }
    couchdb <-  couch.get.url()
    couch_userpwd <- couch.get.authstring()
    uri <- paste(couchdb,db,view,sep="/");
    k <- NULL

    q <- NULL
    if(include.docs){
        q <- 'include_docs=true'
    }

    if(is.null(names(keys))||length(names(keys)) == 1){
        ## in this case, just pass as keys
        thekeys <- rjson::toJSON(keys[[names(keys)]])
        k <- paste('{"keys":',thekeys,'}',sep='')
    }else{
        ## split keys as body json, others as params
        thekeys <- rjson::toJSON(keys$keys)
        k <- paste('{"keys":',thekeys,'}',sep='')
        keys$keys <- NULL
        query <- keys
        qnames <- names(query)
        if(length(qnames)>0){
            for(i in 1:length(qnames)){
                qi <- paste(
                    qnames[i],
                    RCurl::curlEscape(
                        rjson::toJSON(
                            query[[i]]
                            )
                        ),
                    sep='='
                    )
                q <- paste(c(q,qi),collapse='&')
            }
        }
    }
    uri <- paste(uri,q,sep='?')
    reader <- RCurl::basicTextGatherer()
    if(is.null(couch_userpwd)){
        RCurl::curlPerform(
            url = uri
           ,customrequest = "POST"
           ,httpheader = c('Content-Type'='application/json')
           ,postfields = k
           ,writefunction = reader$update
           ,curl=h
            )
    }else{
        RCurl::curlPerform(
            url = uri
           ,customrequest = "POST"
           ,httpheader = c('Content-Type'='application/json')
           ,postfields = k
           ,writefunction = reader$update
           ,curl=h
           ,userpwd=couch_userpwd
            )
    }
    rjson::fromJSON(reader$value()[[1]])
}


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
##' @param txt some txt
##' @param max the max.  Oh my.  send money by wire.
##' @param value the value of it all is without Nigerian priceless
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
##' @param chunksize defaults to 1000.  How many docs to write at a time
##' @param h an RCurl handle, or not (will create one)
##' @return 1, or die trying
##' @export
##' @author James E. Marca
couch.bulk.docs.save <- function(db,
                                 docdf,
                                 chunksize=1000,
                                 h=RCurl::getCurlHandle()){
    if(missing(h)){
        res <- couch.session(h)
    }

    ## in case there are existing docs, have to first fetch all the doc revisions
    varnames <- names(docdf)
    existing_docs <- NULL
    if('_id' %in% varnames){
        ## fetch the corresponding revisions and insert now
        id_rev <- couch.allDocsPost(db,keys=docdf['_id'],include.docs=FALSE,h=h)
        found <- plyr::ldply(id_rev$rows,function(r){
            ## print('processing in llply')
            if(!is.null(r$error)){
                return (NULL)
            }
            data.frame('_id'=r$key,'_rev'=r$value$rev)
        })
        if(!is.na(dim(found)) && dim(found)[1]>0){
            colnames(found) <- c('_id','_rev')
            docdf <- merge(docdf,found,all=TRUE)
        }
    }

    ## here I assume that docdf is a datafame
    config <- get.config()$couchdb
    if(missing(db)){
        db <- config$trackingdb
    }else{
        if(length(db)>1){
            db <- couch.makedbname(db)
        }
    }
    couchdb <-  couch.get.url()
    ## the bulk docs target
    uri <- paste(couchdb,db,'_bulk_docs',sep="/")

    ## push 1000 at a time
    i <- chunksize
    maxi <- length(docdf[,1])
    if(i > maxi ) i <- maxi

    j <- 1

    docspushed <- 0

    reader = nullTextGatherer()

    ## for debugging, use the following, but it fills up for nothing
    ## if you're dumping thousands of docs
    ## reader <- RCurl::basicTextGatherer()

    ## sort columns into numeric and text
    num.cols <-  unlist(plyr::llply(docdf[1,],is.numeric))
    txt.cols <- unlist(plyr::llply(docdf[1,],is.character))
    oth.cols <- ! (num.cols | txt.cols)
    num.cols <- varnames[num.cols]
    txt.cols <- varnames[txt.cols]
    oth.cols <- varnames[oth.cols]

    while(length(docdf)>0) {
        chunk <- docdf[j:i,]
        if( i >= length(docdf[,1]) ){
            docdf <- data.frame()
        }else{
            docdf <- docdf[-j:-i,]
        }
        ## for next iteration
        if(length(docdf) && i > length(docdf[,1])) i <- length(docdf[,1])
        bulkdocs <- jsondump6(chunk,num.cols=num.cols,txt.cols=txt.cols,oth.cols=oth.cols)
        ## print(bulkdocs)
        curlresult <- try( RCurl::curlPerform(
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
            h = RCurl::getCurlHandle()
            couch.session(h)
            RCurl::curlPerform(
                url = uri
               ,httpheader = c('Content-Type'='application/json')
               ,customrequest = "POST"
               ,postfields = bulkdocs
               ,writefunction = reader$update
               ,curl = h
                )
        }
        docspushed <- docspushed + length(chunk[,1])
    }
    # print(reader$value())
    docspushed
}
