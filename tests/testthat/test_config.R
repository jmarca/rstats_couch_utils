library('rcouchutils')

test_that("can load the test.config.file",{

    config <- get.config()

    expect_that(config$dbname,equals('vdsdata'))

})
