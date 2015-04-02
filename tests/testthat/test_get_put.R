library('rcouchutils')
pkg <- as.package('.')
Sys.setenv(RCOUCHDBUTILS_CONFIG_FILE=paste(pkg$path,'test.config.json',sep='/'))

parts <- c('get','put','post')
couch.makedb(parts)

test_that("can post and get",{

    doc <- list()
    doc[['one potato']] <- 'two potatoes'
    doc$beer <- 'food group'
    doc$foo <- 123

    post_result <- couch.post(parts,doc)

    expect_that(post_result,is_a('list'))
    expect_that(names(post_result),equals(c('ok','id','rev')))
    expect_that(post_result$ok,equals(TRUE))

    doc <- couch.get(parts,post_result$id)

    expect_that(doc,is_a('list'))
    expect_that(names(doc),equals(c('_id','_rev',
                                           'one potato',
                                           'beer',
                                           'foo')))
    expect_that(doc[['one potato']],equals('two potatoes'))
    expect_that(doc$beer,equals('food group'))
    expect_that(doc$foo,equals(123))

})

test_that("can put and delete",{

    doc <- list()
    doc[['one potato']] <- 'two potatoes'
    doc$beer <- 'food group'
    doc$foo <- 123

    id <- 'frightful waste'
    put_result <- couch.put(parts,
                            docname=id,
                            doc=doc )


    expect_that(put_result,is_a('list'))
    expect_that(names(put_result),equals(c('ok','id','rev')))
    expect_that(put_result$ok,equals(TRUE))
    expect_that(put_result$id,equals('frightful waste'))

    doc <- couch.get(parts,put_result$id)

    expect_that(doc,is_a('list'))
    expect_that(names(doc),equals(c('_id','_rev',
                                           'one potato',
                                           'beer',
                                           'foo')))
    expect_that(doc[['one potato']],equals('two potatoes'))
    expect_that(doc$beer,equals('food group'))
    expect_that(doc$foo,equals(123))

    head_result <- couch.head(parts,docname=id)
    expect_that( names(head_result),
                equals(c("Server","ETag","Date","Content-Type","Content-Length",
                         "Cache-Control","status","statusMessage")))

    del_result <- couch.delete(parts,
                               docname=id
                               )
    expect_that(del_result,is_a('list'))
    expect_that(names(del_result),equals(c('ok','id','rev')))
    expect_that(del_result$ok,equals(TRUE))
    expect_that(del_result$id,equals('frightful waste'))

    ## can't get it again
    doc <- couch.get(parts,put_result$id)

    expect_that(doc,is_a('list'))
    expect_that(names(doc),equals(c('error','reason')))
    expect_that(doc$error,equals("not_found"))
    expect_that(doc$reason,equals("deleted"))

})

test_that("can set and check state",{

    dbname <- couch.makedbname(parts)
    doc <- list()
    doc[['one potato']] <- 'two potatoes'
    doc$beer <- 'food group'
    doc$foo <- 123

    id <- 'franged123'

    set_result <- couch.set.state(year='belsh',
                                  id=id,
                                  doc=doc,
                                  db=dbname
                                  )

    expect_that(set_result,is_a('list'))
    expect_that(names(set_result),equals(c('ok','id','rev')))
    expect_that(set_result$ok,equals(TRUE))
    expect_that(set_result$id,equals('franged123'))

    state.check <- couch.check.state(year='belsh',
                                     id=id,
                                     state='beer',
                                     db=dbname
                                     )

    expect_that(state.check,equals('food group'))

    state.check <- couch.check.state(year='belsh',
                                     id=id,
                                     state='impute trucks',
                                     db=dbname
                                     )
    expect_that(state.check,equals('todo'))

})
couch.deletedb(parts)
