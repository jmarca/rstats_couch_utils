test_that("can load the test.config.file",{

    config <- get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
##    print (config)
    expect_that(config,is_a('list'))

    config2 <- get.config()
    expect_that(config,equals(config2))
})


test_that("helpful utilities",{
    config <- get.config()

    ## yes this code is identical to the actual code, but I don't know
    ## another way to do this

    expect_that(couch.get.url(),equals(paste("http://",
                                             config$couchdb$host,
                                             ":",
                                             config$couchdb$port,
                                             sep='')))
    if(is.null(config$couchdb$auth) ||
       is.null(config$couchdb$auth$username) ||
       is.null(config$couchdb$auth$password) ){
        expect_null(couch.get.authstring())
    }else{

        authstr <- couch.get.authstring()
        expect_that(strsplit(authstr,":")[[1]],
                    equals(c(config$couchdb$auth$username,
                             config$couchdb$auth$password))
                    )
    }

})
