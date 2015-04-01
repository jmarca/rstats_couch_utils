library('rcouchutils')
pkg <- as.package('.')
Sys.setenv(RCOUCHDBUTILS_CONFIG_FILE=paste(pkg$path,'test.config.json',sep='/'))

test_that("make db name stuff works okay",{
    parts <- c('a','b')
    config <- get.config()
    expect_that(couch.makedbname(parts),
                equals(paste(config$dbname,"%2fa%2fb",sep='')))

    expect_that(couch.makedbname.noescape(parts),
                equals(paste(config$dbname,"/a/b",sep='')))

})
