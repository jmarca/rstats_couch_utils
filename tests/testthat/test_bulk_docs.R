config <- get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))

parts <- c('bulk','docs')
couch.makedb(parts)
dbname <-  couch.makedbname(parts)

context('retrieving')
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

context('putting with bulk docs')
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

## build a list version
lgrid <- list()
for(i in 1:length(grid$vol)){
    lgrid[[i]]=list(
        '_id'=grid[i,'_id'],
        'x'=grid$x[i],
        'y'=grid$y[i],
        'newvol'=grid$vol[i],
        'newocc'=grid$occ[i],
        'attr'=list('v'=grid$vol[i],
                      'o'=grid$occ[i]))
}

test_that('bulk doc works',{


    ## but let's not croak all at once!
    res <- couch.bulk.docs.save(parts,head(grid))
    ## print (res)
    expect_that(res,equals(length(head(grid[,1]))))

    ## And it also works with duplicate docs.

    res <- couch.bulk.docs.save(parts,head(grid))
    expect_that(res,equals(length(head(grid[,1]))))

    ## bigger chunk, some already in
    res <- couch.bulk.docs.save(parts,grid[1:10,])
    testthat::expect_that(res,equals(10))

    storedids <- grid[1:12,'_id']
    getstored <- couch.allDocsPost(parts,keys=storedids,include.docs=TRUE)

    ## first 1:10 should be good, last two not found
    for(i in 1:10){
        row <- getstored$rows[[i]]$doc
        testthat::expect_equal(as.numeric(row$vol),grid$vol[i], tolerance = .002)
        testthat::expect_equal(as.numeric(row$occ),grid$occ[i], tolerance = .002)
    }
    testthat::expect_true(! is.null(getstored$rows[[11]]$error) )
    testthat::expect_true(! is.null(getstored$rows[[12]]$error) )

    ## list version of saving, only one new
    res <- couch.bulk.docs.save(parts,lgrid[1:11])
    expect_that(res,equals(11))

    ## can get them back

    getstored <- couch.allDocsPost(parts,keys=storedids,include.docs=TRUE)

    ## last one should be not found
    ## first 1:10 should not have vol, occ
    for(i in 1:11){
        ## print(paste('row',i))
        row <- getstored$rows[[i]]$doc
        testthat::expect_null(row$vol)
        testthat::expect_null(row$occ)
        testthat::expect_equal(as.numeric(row$newvol),grid$vol[i], tolerance = .002)
        testthat::expect_equal(as.numeric(row$newocc),grid$occ[i], tolerance = .002)
        testthat::expect_equal(row$newvol,lgrid[[i]]$newvol)
        testthat::expect_equal(row$newocc,lgrid[[i]]$newocc)
    }
    testthat::expect_true(! is.null(getstored$rows[[12]]$error) )

})

context(paste('stand by for big bulk_docs save of ',
                length(grid[,1]),
               'documents'))

test_that('big bulk doc works',{

    res <- couch.bulk.docs.save(parts,grid)
    expect_that(res,equals(length(grid[,1])))

    res <- couch.bulk.docs.save(parts,lgrid)
    expect_that(res,equals(length(grid[,1])))

})

test_that('can save nested structures okay',{

    env <- new.env()
    res <- load(file='./tests/testthat/storedf.rda',envir = env)
    testlist <- env[[res]]

    res <- couch.bulk.docs.save(parts,testlist)
    expect_that(res,equals(24))

    ids <-  plyr::laply(testlist,function(row){return (row[['_id']])})
    getstored <- couch.allDocsPost(parts,keys=ids,include.docs=TRUE)

    for(i in 1:length(ids)){
        row <- getstored$rows[[i]]$doc
        testthat::expect_is(row$aadt,'list')
        testthat::expect_equal(length(row$aadt),3)
    }

})

couch.deletedb(parts)
