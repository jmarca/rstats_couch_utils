library('rcouchutils')
pkg <- as.package('.')
Sys.setenv(RCOUCHDBUTILS_CONFIG_FILE=paste(pkg$path,'test.config.json',sep='/'))

parts <- c('bulk','docs')
couch.makedb(parts)
dbname <-  couch.makedbname(parts)

test_that("can retrieve all docs",{

    doc <- list()
    doc[['one potato']] <- 'two potatoes'
    doc$beer <- 'food group'
    doc$foo <- 123

    someids <- c('saute','eggs','bacon','sausage')

    for(id in someids){
        couch.put(parts,
                  docname=id,
                  doc=doc )
    }

    ## now use allDocs without a query
    res <- couch.allDocs(dbname)
    expect_that(res,is_a('list'))
    expect_that(names(res),equals(c('total_rows','offset','rows')))
    expect_that(res$total_rows,equals(4))
    expect_that(res$offset,equals(0))
    expect_that(length(res$rows),equals(4))
    for(row in res$rows){
        expect_that(names(row),equals(c('id','key','value','doc')))
        expect_that(names(row$doc),equals(c('_id','_rev',
                                            'one potato',
                                            'beer',
                                            'foo')))
        expect_that(row$doc[['one potato']],equals('two potatoes'))
        expect_that(row$doc$beer,equals('food group'))
        expect_that(row$doc$foo,equals(123))
        expect_true(row$id %in% someids)
    }

    ## now with a query parameter
    res <- couch.allDocs(dbname,query=list('startkey'='s','endkey'='t'))
    ## print(res)
    expect_that(res,is_a('list'))
    expect_that(names(res),equals(c('total_rows','offset','rows')))
    expect_that(res$total_rows,equals(4))
    expect_that(res$offset,equals(2))
    expect_that(length(res$rows),equals(2))
    for(row in res$rows){
        expect_that(names(row),equals(c('id','key','value','doc')))
        expect_that(names(row$doc),equals(c('_id','_rev',
                                            'one potato',
                                            'beer',
                                            'foo')))
        expect_that(row$doc[['one potato']],equals('two potatoes'))
        expect_that(row$doc$beer,equals('food group'))
        expect_that(row$doc$foo,equals(123))
        expect_true(row$id %in% c('saute','sausage'))
    }

    ## now with keys
    res <- couch.allDocsPost(dbname,
                             keys=
                                 list('keys'=c('eggs','saute'),
                                      'descending'=TRUE))
    expect_that(res,is_a('list'))
    expect_that(names(res),equals(c('total_rows','offset','rows')))
    expect_that(res$total_rows,equals(4))

    expect_that(length(res$rows),equals(2))
    expect_that(res$rows[[1]]$id,equals('saute')) ## descending
    for(row in res$rows){
        expect_that(names(row),equals(c('id','key','value','doc')))
        expect_that(names(row$doc),equals(c('_id','_rev',
                                            'one potato',
                                            'beer',
                                            'foo')))
        expect_that(row$doc[['one potato']],equals('two potatoes'))
        expect_that(row$doc$beer,equals('food group'))
        expect_that(row$doc$foo,equals(123))
        expect_true(row$id %in% c('saute','eggs'))
    }


})

test_that('bulk doc works',{

    ## generate a lot of "sites"
    grid <- NULL
    for (gridx in 1:10){
        for( gridy in 1:10 ){
            ts <-  seq(from=as.POSIXct('2012-01-01'),
                           to=as.POSIXct('2012-01-10'),
                           by=3600)
            grid <- rbind(grid,data.frame(x=gridx,y=gridy,ts=ts))
        }

    }
    grid$vol <- rnorm(length(grid[,1]),mean=200,sd=10)
    grid$occ <- runif(length(grid[,1]))
    grid[,'_id'] <- paste(grid$x,grid$y,grid$ts,sep='_')

    ## that's 21,700 rows we can write

    ## but let's not croak all at once!
    res <- couch.bulk.docs.save(parts,head(grid))
    ## print (res)
    expect_that(res,equals(length(head(grid[,1]))))

    ## And it also works with duplicate docs.

    res <- couch.bulk.docs.save(parts,head(grid))
    expect_that(res,equals(length(head(grid[,1]))))

    ## bigger chunk, have already in, half not
    res <- couch.bulk.docs.save(parts,grid[1:10,])
    expect_that(res,equals(10))

    ## and the rest.  choke on that, CouchDB!
    print(paste('stand by for big bulk_docs save of ',
                length(grid[,1]),
                'documents'))
    res <- couch.bulk.docs.save(parts,grid)
    expect_that(res,equals(length(grid[,1])))

})

couch.deletedb(parts)
