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

})

couch.deletedb(parts)
