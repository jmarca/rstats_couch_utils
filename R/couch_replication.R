##' start replication between two couchdb databases
##'
##' This is going to be tricky because my config.json file at the
##' moment only has a slot for a single couchdb in it.
##'
##' But the idea is that you can trigger replication from one db on
##' machine A to another db on machine B, where B might be the same as
##' A.  Without leaving the comfort of R, whatever that means.
##'
##' @title couch.start.replication
##' @param src the source database
##' @param tgt the target database
##' @param id the desired id for the replication document.  Can be
##' NULL or skipped if you're happy letting couchdb assign its own
##' UUID.  Note that if the id points to a replication document that
##' already exists, this routine will first delete that replication
##' document and then overwrite it.  This is because you can't modify
##' an existing replication if it is already triggered.
##' @param continuous TRUE or FALSE, should the replication be
##' continuous (TRUE) or should it just be a one shot copy deal
##' (FALSE)
##' @param create_target TRUE to create the database on the target
##' machine if it does not yet exist, FALSE to \emph{not} create the
##' target database if it does not exist, and instead have the
##' replication fail.  If you choose FALSE, that is safe, but note
##' that this script will not monitor the replication, and so will not
##' report back any error conditions if the replication fails because
##' the target database does not exist.
##'
##' Yes I could write that I guess, but I don't feel like it at the
##' moment.
##' @return the result of PUTting (if there is a user defined id) or
##' POSTing (if there isn't a user-defined id) the replication
##' document into the source database
##' @author James E. Marca
couch.start.replication <- function(src,tgt,
                                    id=NULL,
                                    continuous=FALSE,
                                    create_target=FALSE
                                    ){

    h = RCurl::getCurlHandle()
    res <- couch.session(h)

    doc = list("source" = src
       ,"target" = tgt
       ,"create_target" = create_target
       ,"continuous" = continuous
       ,"user_ctx" = list( "name"=res$name, "roles"=res$roles )
               )
    if(!is.null(id)){
        current <- couch.delete('_replicator',id,h=h)
    }
    print(paste("setting up replication doc:\n",rjson::toJSON(doc)))
    result='ok'
    if(is.null(id)){
        ## no id, use post
        result <-
            couch.post('_replicator'
                      ,doc
                      ,h=h
                       )

    }else{
        result <-
            couch.put('_replicator'
                     ,id
                     ,doc
                     ,h=h
                      )

    }
    result
}
