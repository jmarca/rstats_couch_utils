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
