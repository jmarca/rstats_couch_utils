library('rcouchutils')
pkg <- as.package('.')
Sys.setenv(RCOUCHDBUTILS_CONFIG_FILE=paste(pkg$path,'test.config.json',sep='/'))

test_that("can establish a session",{

    h <- RCurl::getCurlHandle()

    res <- couch.session(h)

    expect_that(res,is_a('list'))
    expect_that(names(res),equals(c('ok','name','roles')))
    expect_that(res$ok,equals(TRUE))

})
