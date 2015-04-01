library('rcouchutils')
pkg <- as.package('.')
Sys.setenv(RCOUCHDBUTILS_CONFIG_FILE=paste(pkg$path,'test.config.json',sep='/'))

test_that("can load the test.config.file",{

    config <- get.config()

    expect_that(config$dbname,equals('vdsdata'))

})


test_that("helpful utilities",{
    config <- get.config()
    ## yes this code is identical to the actual code, but I don't know
    ## another way to do this

    expect_that(couch.get.url(),equals(paste("http://",
                                             config$host,
                                             ":",
                                             config$port,
                                             sep='')))
    if(is.null(config$auth) ||
       is.null(config$auth$username) ||
       is.null(config$auth$password) ){
        expect_null(couch.get.authstring())
    }else{

        authstr <- couch.get.authstring()
        expect_that(strsplit(authstr,":")[[1]],
                    equals(c(config$auth$username,
                             config$auth$password))
                    )
    }

})
