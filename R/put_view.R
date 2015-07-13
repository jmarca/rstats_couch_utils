##' Put a new design doc into the couchdb database.
##'
##' This is more or less identical code to the attachments ftp upload
##' stuff, but with _design/ in the name rather than pushing to the
##' attachments endpoints
##'
##' @title couch.put.view
##' @param db the target database
##' @param design the name of the design document, as a plain string.
##'     if the name you pass in is something like "blargle", and the
##'     db string is "posh", then the document path for the design
##'     document will be "/posh/_design/blargle"
##' @param docfile the actual document to store.  Because I am super
##'     cranky this morning, and because JS scripts are not R code,
##'     and never the twain shall meet, I'm just going to assume that
##'     the easiest way for everybody (me now and me in the future) is
##'     to load a JSON doc from a file.  So pass in a complete
##'     filename here, and curl will just upload it directly.
##' @param h a curl handle.  Optional
##' @author James E. Marca
##' @export
##'
couch.put.view <- function(db,design,docfile,
                           h=RCurl::getCurlHandle()){

    if(length(db)>1){
        db <- couch.makedbname(db)
    }
    couchdb <-  couch.get.url()
    docname <- paste("_design",
                     RCurl::curlEscape(design),
                     sep="/")
    if(missing(h)){
        couch.session(h)
    }
    doc_rev <- get.rev.from.head(db,docname,escape_docname=FALSE)

    if(is.na(doc_rev)){
        ## can't use existing connection here.  Curl pukes
        result <- couch.put(db,docname,doc="{}")
        doc_rev <- result$rev
    }
    uri <- paste(couchdb,db,docname,
                 sep="/")
    uri <- paste(uri,paste('rev',doc_rev,sep='='),sep='?')

    content.type <- "application/json"

    reader = RCurl::basicTextGatherer()

    res <- RCurl::ftpUpload(what=docfile, to=uri
                           ,httpheader = c('Content-Type'=content.type[[1]])
                           ,customrequest='PUT'
                           ,writefunction = reader$update
                           ,curl=h
                            )
    response <-  reader$value()
    if(!is.null(response) && response != ''){
        response <- rjson::fromJSON(response)
    }
    response
}
