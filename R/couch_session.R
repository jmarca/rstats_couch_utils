##' set up a session with couchdb in a persistent curl handle
##'
##' session isn't a json post, so has its own call to curlPerform
##'
##' Will use ".cookies.txt" as the cookie jar for the session
##'
##' @title couch.session
##' @param h a persistent curl handle to preserve this session. If you
##' don't pass this in, then what is the point of setting up a
##' session?  A session eliminates the need to keep establishing a
##' connection and all that, and sometimes helps with stuff like
##' writing to replication database
##' @return the result of setting up the session
##' @author James E. Marca
couch.session <- function(h){
  RCurl::curlSetOpt(cookiejar='.cookies.txt', curl=h)
  couchdb <- couch.get.url()
  uri <- paste(couchdb,"_session",sep="/")
  reader = RCurl::basicTextGatherer()
  config <-  get.config()
  if(is.null(config$auth) ||
     is.null(config$auth$username) ||
     is.null(config$auth$password)){
      ## canna do nae
      return(NULL)
  }
  authstring <- paste(paste('name',config$auth$username,sep='='),
                      paste('password',config$auth$password,sep='='),
                      sep='&')

  RCurl::curlPerform(
      url = uri
     ,customrequest = "POST"
     ,writefunction = reader$update
     ,postfields = authstring
     ,curl=h
     ,httpauth='ANY'
              )
  RJSONIO::fromJSON(reader$value(),simplify=FALSE)
}
