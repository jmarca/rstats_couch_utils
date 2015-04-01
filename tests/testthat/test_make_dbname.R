library('rcouchutils')
pkg <- as.package('.')
Sys.setenv(RCOUCHDBUTILS_CONFIG_FILE=paste(pkg$path,'test.config.json',sep='/'))

test_that("make db name stuff works okay",{
    parts <- c('a','b')

    expect_that(couch.makedbname(parts),equals("vdsdata%2fa%2fb"))

    expect_that(couch.makedbname.noescape(parts),equals("vdsdata/a/b"))

})
