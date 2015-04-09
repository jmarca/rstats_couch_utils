config <- get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))

test_that("can make and delete a db",{
    parts <- c('a','b')

    result <- couch.makedb(parts)
    expect_that(result$ok,equals(TRUE))

    result <- couch.makedb(parts)
    expect_null(result$ok)
    expect_that(result$error,equals("file_exists"))
    expect_that(result$reason,equals("The database could not be created, the file already exists."))

    result <- couch.deletedb(parts)
    expect_that(result$ok,equals(TRUE))

})
