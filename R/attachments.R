##library(RCurl) ## need to do this for mimeTypeExtensions
##data('mimeTypeExtensions',package='RCurl')

##' Attach something like a file or whatnot to a couchdb document
##'
##' @title couch.attach
##' @param db the database name, or its component parts as a list
##' @param docname the document id to which you want to attach.  It is
##' okay to pass a new docname.  If the doc does not yet exist in the
##' database, then a new document will be created and the file
##' attached to that new document with the correct id.
##' @param attfile the file that you want to attach.  Note that the
##' last part of the file name will be URL escaped, and will serve as
##' the name of the thing that is attached.  So for example if you
##' pass in an attfile something like
##'      /tmp/files/plots/my coolplot.png
##' then the stored object will be named
##'      my%20cool%20plot.png
##' because HTTP doesn't allow spaces, and
##' couchdb follows HTTP.
##' @param h an existing RCurl handle.  will create one if not passed in
##' @return the result of attaching.  Hopefully a JSON object that
##' says okay
##' @export
##' @author James E. Marca
couch.attach <- function(db,
                         docname,
                         attfile,
                         h=RCurl::getCurlHandle()){
    if(missing(h)){
        couch.session(h)
    }

    file.path <- unlist(strsplit(attfile,"/"))
    flen <- length(file.path)
    filename <- file.path[flen]

    if(length(db)>1){
        db <- couch.makedbname(db)
    }
    couchdb <-  couch.get.url()
    uri <- paste(couchdb,db,
               ## remove spaces in url or doc id
               RCurl::curlEscape(docname),
               RCurl::curlEscape(filename),
               sep="/");
    ## couch_userpwd <- couch.get.authstring()

    doc_rev <- get.rev.from.head(db,docname,h=h)
    if(is.na(doc_rev)){
        ## can't use existing connection here.  Curl pukes
        result <- couch.put(db,docname,doc="{}")
        doc_rev <- result$rev
    }
    uri <- paste(uri,paste('rev',doc_rev,sep='='),sep='?')

    content.type <- RCurl::guessMIMEType(attfile, "application/x-binary")
    ## debugging
    ## assemble an equivalent curl command line, for testing
    ## commandLine <- paste('curl -vX PUT ',uri,' --data-binary @',attfile,
    ##                      ' -H "Content-Type: ',content.type[[1]],'"',sep='')
    ## cat (commandLine)
    
    reader = RCurl::basicTextGatherer()
    
    res <- RCurl::ftpUpload(what=attfile, to=uri
                           ,httpheader = c('Content-Type'=content.type[[1]])
                           ,customrequest='PUT'
                           ##,verbose=TRUE
                           ,upload=TRUE
                           ,writefunction = reader$update
                           ,curl=h
                            )
    
    ## apparently, CFILE doesn't want to work anymore
    ## f <- RCurl::CFILE(filename=attfile)
    ## res <- RCurl::curlPerform(url = uri,
    ##                           customrequest='PUT',
    ##                           upload = TRUE,
    ##                           writefunction = reader$update,
    ##                           readdata = f@ref,
    ##                           infilesize = file.info(attfile)[1, "size"],
    ##                           httpheader = c('Content-Type'=content.type[[1]]),
    ##                           curl=h
    ##                           ,verbose=TRUE
    ##                           )
  
    rjson::fromJSON(reader$value())

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
couch.get.attachment <- function(db='vdsdata%2ftracking',docname,attachment){
    if(length(db)>1){
        db <- couch.makedbname(db)
    }
    couchdb <-  couch.get.url()
    uri <- paste(couchdb,db,
               ## remove spaces in url or doc id
               RCurl::curlEscape(docname),
               RCurl::curlEscape(attachment),
               sep="/");

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
##' @export
##' @author James E. Marca
couch.has.attachment <- function(db,docname,attachment){
  r <- couch.get(db,docname)
  attachments <- r[['_attachments']]
  attachment %in% names(attachments)
}
