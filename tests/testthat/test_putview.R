config <- get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))

parts <- c('views','testing')
couch.makedb(parts)
dbname <-  couch.makedbname(parts)

for (id in c('one','two','three','50')){
    doc <- list()
    doc[['one potato']] <- 'two potatoes'
    doc$beer <- 'food group'
    doc$foo <- 123
    doc$bar <- paste('foo',id,sep='')
    put_result <- couch.put(parts,
                            docname=id,
                            doc=doc )
}

test_that("can save a design doc to a file",{

    res <- couch.put.view(dbname,'theview','./files/view1.json')
    expect_that(res,is_a('list'))
    expect_that(names(res),equals(c('ok','id','rev')))
    expect_that(res$ok,equals(TRUE))

    ## query the stored view, make sure it works
    res <- couch.allDocs(dbname,view='_design/theview/_view/barfoo'
                        ,query=list('reduce'=FALSE
                                    )
                         ,include.docs=FALSE)

    expect_that(res,is_a('list'))
    expect_that(names(res),equals(c('total_rows','offset','rows')))
    expect_that(res$total_rows,equals(2))
    for(row in res$rows){
        expect_that(names(row),equals(c('id','key','value')))
        expect_that(row$value,equals(1))
        expect_that(row$id %in% c('50','one'),is_true())
    }

})


## couch.deletedb(parts)
