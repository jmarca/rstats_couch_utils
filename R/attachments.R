library(RCurl) ## need to do this for mimeTypeExtensions

couch.attach <- function(db=trackingdb,
                         docname,
                         attfile,
                         h=RCurl::getCurlHandle()){

    file.path <- unlist(strsplit(attfile,"/"))
    flen <- length(file.path)
    filename <- file.path[flen]

    if(length(db)>1){
        db <- couch.makedbname(db)
    }
    couchdb <-  couch.get.url()
    uri <- paste(couchdb,db,docname,filename,sep="/")
    couch_userpwd <- couch.get.authstring()

    doc_rev <- get.rev.from.head(db,docname,h)

    uri <- paste(uri,paste('rev',doc_rev,sep='='),sep='?')
    ## remove spaces in url or doc id
    uri <- gsub("\\s","%20",x=uri,perl=TRUE)

    ## print(paste('putting attachment using blobity blob to',uri))

    ##couch.session(h)
    content.type <- RCurl::guessMIMEType(attfile, "application/x-binary")

    reader = RCurl::basicTextGatherer()
    f <- RCurl::CFILE(filename=attfile)
    res <- curlPerform(url = uri,
                       customrequest='PUT',
                       upload = TRUE,
                       writefunction = reader$update,
                       readdata = f@ref,
                       infilesize = file.info(attfile)[1, "size"],
                       httpheader = c('Content-Type'=content.type[[1]])
                      ##,verbose=TRUE
                       )

    RJSONIO::fromJSON(reader$value(),simplify=FALSE)

    ## putting.command <- paste('curl',
    ##                          paste('-v -X PUT -H "Content-Type: ',
    ##                                content.type,'" ',
    ##                                uri,' --data-binary @',
    ##                                attfile,
    ##                                sep='')
    ##                          )
    ## ## have to wait, in case there are other docs to attach
    ## ## until I figure out how to multiple at a time deal thingee

    ## r <- try(
    ##     print(system2('curl',paste('-v -X PUT -H "Content-Type: ',content.type,'" ',uri,' --data-binary @',attfile,sep=''),wait=TRUE ,stdout=TRUE,stderr=TRUE))
    ##     )
    ## if(class(r) == "try-error") {
    ##     print('doit later')
    ##     ## make a note of it
    ##     cat(paste('couch.attach failed:',putting.command,'\n' ),file='failedcurl.log',append=TRUE)
    ## }else{
    ##     print('success')
    ## }

}

##' Retrieve an attachment from CouchDB document
##'
##' This function is used almost exclusively to fetch RData files that
##' have been attached to CouchDB document.  The way it works is to
##' call out to curl via system2, save the file in a temp directory
##' with a tempfile name, and then load the file up from there.  This
##' works.  Not pretty, but it is fairly robust.
##'
##'
##' This will get the requested attachment.  I perhaps should stick
##' with using RCurl, but I'm so sick of wrestling with it tonight
##' that I'm going to keep this function the old way of just using
##' system2 and curl straight.  The advantage is that this lets me
##' load the RData file into R directly, if I want, or whatever.
##'
##'
##' @title couch.get.attachment.2
##' @param db the database
##' @param docname the document id
##' @param attachment the name of the attachment to fetch
##' @return nothing at all this is broken
##' @author James E. Marca
couch.get.attachment.broken <- function(db=trackingdb,docname,attachment){
    if(length(db)>1){
        db <- couch.makedbname(db)
    }
    couchdb <-  couch.get.url()
    uri <- paste(couchdb,db,docname,attachment,sep="/")
    uri=gsub("\\s","%20",x=uri,perl=TRUE)

    ## taken from the RCurl manual
    h = RCurl::basicTextGatherer()
    content = RCurl::getBinaryURL(
        url=uri,
        .opts = list(headerfunction = h$update)
        )
    header = parseHTTPHeader(h$value())
    type = strsplit(header["Content-Type"], "/")[[1]]
    if(type[2] %in% c("x-gzip", "gzip")) {
        if(require(Rcompression))
            x = gunzip(content)
    }


    tmp <- tempfile(paste('remotedata',attachment,sep='_'))

    file.create(tmp)

    ## print(paste('getting attachment',uri))
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

##' Retrieve an attachment from CouchDB document
##'
##' This function is used almost exclusively to fetch RData files that
##' have been attached to CouchDB document.  The way it works is to
##' call out to curl via system2, save the file in a temp directory
##' with a tempfile name, and then load the file up from there.  This
##' works.  Not pretty, but it is fairly robust.
##'
##'
##' This will get the requested attachment.  I perhaps should stick
##' with using RCurl, but I'm so sick of wrestling with it tonight
##' that I'm going to keep this function the old way of just using
##' system2 and curl straight.  The advantage is that this lets me
##' load the RData file into R directly, if I want, or whatever.
##'
##'
##' @title couch.get.attachment
##' @param db the database
##' @param docname the document id
##' @param attachment the name of the attachment to fetch
##' @return a list containing a single entry with the old variable
##' name that equals the environment into which that variable was
##' loaded, so that you can reconsititute the data by doing:
##'
##'     getres <- couch.get.attachment.2(dbname,id,'save.RData')
##'     varnames <- names(getres)
##'     barfl <- getres[[1]][[varnames[1]]]
##'
##' In the above, barfl now holds whatever data was originally saved
##' in the file, nomatter what that data might have originally been
##' called.  That original variable name can be found by inspecting
##' the value of varnames[1], if you want.
##' @export
##' @author James E. Marca
couch.get.attachment <- function(db=trackingdb,docname,attachment){
    if(length(db)>1){
        db <- couch.makedbname(db)
    }
    couchdb <-  couch.get.url()
    uri <- paste(couchdb,db,docname,attachment,sep="/")
    uri=gsub("\\s","%20",x=uri,perl=TRUE)

    tmp <- tempfile(paste('remotedata',attachment,sep='_'))

    file.create(tmp)

    ## print(paste('getting attachment',uri))
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

    env <- new.env()
    res <- load(tmp, env)
    unlink(tmp)
    result <- list()
    result[[res]]=env
    return (result)
}


##' Check if a couchdb document had a given attachment already
##'
##' The idea is to be able to check and see if an expected attachment
##' is already uploaded to the document.  This is needed for example
##' to see whether or not to generate figures, or whether to attach
##' RData files, etc.
##'
##' If it is there, you will get TRUE, if not, FALSE
##'
##' Note that this only compares the name of the attachment, not the
##' content
##'
##' @title couch.has.attachment
##' @param db the database
##' @param docname the document id
##' @param attachment the name of the attachment you are looking for
##' @return TRUE if attachment is there, FALSE if not
##' @author James E. Marca
couch.has.attachment <- function(db,docname,attachment){
  r <- couch.get(db,docname)
  attachments <- r[['_attachments']]
  attachment %in% names(attachments)
}
