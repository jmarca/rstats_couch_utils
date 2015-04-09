config <- get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))

test_that("make db name stuff works okay",{
    parts <- c('a','b')
    config <- get.config()$couchdb
    expect_that(couch.makedbname(parts),
                equals(paste(config$dbname,"%2Fa%2Fb",sep='')))

    expect_that(couch.makedbname.noescape(parts),
                equals(paste(config$dbname,"/a/b",sep='')))

})
