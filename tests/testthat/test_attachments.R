config <- get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))

parts <- c('attachments','testing')
couch.makedb(parts)
dbname <-  couch.makedbname(parts)

test_that("can save attachments to a file",{

    doc <- list()
    doc[['one potato']] <- 'two potatoes'
    doc$beer <- 'food group'
    doc$foo <- 123

    id <- 'saute'

    put_result <- couch.put(parts,
                            docname=id,
                            doc=doc )

    expect_that(put_result,is_a('list'))
    expect_that(names(put_result),equals(c('ok','id','rev')))
    expect_that(put_result$ok,equals(TRUE))

    ## now attach
    res <- couch.attach(dbname,id,'./files/fig.png')
    expect_that(res,is_a('list'))
    expect_that(names(res),equals(c('ok','id','rev')))
    expect_that(res$ok,equals(TRUE))

    hasit <- couch.has.attachment(dbname,id,'fig.png')
    expect_that(hasit,equals(TRUE))

    ## attach sample RData file
    tmpdf <- data.frame(x=c(1,2,3,4,5),
                        y=c('a','b','c','d','e')
                        ,stringsAsFactors=FALSE)
    file_location <- paste(tempdir(),'save.RData',sep='/')
    save(tmpdf,file=file_location,compress='xz')
    res <- couch.attach(dbname,id,file_location)

    rm(tmpdf)
    tmpdf <- NULL
    expect_that(tmpdf,is_null())


    expect_that(res,is_a('list'))
    expect_that(names(res),equals(c('ok','id','rev')))
    expect_that(res$ok,equals(TRUE))

    hasit <- couch.has.attachment(dbname,id,'save.RData')
    expect_that(hasit,equals(TRUE))

    getres <- couch.get.attachment(dbname,id,'save.RData')

    expect_that(getres,is_a('list'))

    varnames <- names(getres)
    barfl <- getres[[1]][[varnames[1]]]

    expect_that(barfl,is_a('data.frame'))
    expect_that(barfl$x,equals(c(1,2,3,4,5)))
    expect_that(barfl$y,equals(c('a','b','c','d','e')))

})

couch.deletedb(parts)
