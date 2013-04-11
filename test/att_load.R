source('./couchUtils.R')


db = Sys.getenv(c('RTESTDB'))[1]
if(is.null(db)){
    quit(save='no',status=3)
}
print(db)
doc = Sys.getenv(c('RDOCFILE'))[1]
if(is.null(doc)){
  print
  quit(save='no',status=4) 
}
print(doc)

result <- couch.get.attachment(db,doc,paste('attach',doc,sep=''))

print(result)
print(dim(df.merged))

quit(save='no',status=10)
