config <- get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))

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
    expect_that(del_result,equals(1))

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

test_that("can get db info",{
    res <- get.db(parts)

    expect_that(res,is_a('list'))
    resnames <-  sort(names(res))
    api_names <- sort(c(
        'committed_update_seq',
        'compact_running',
        'db_name',
        'disk_format_version',
        'data_size',
        'disk_size',
        'doc_count',
        'doc_del_count',
        'instance_start_time',
        'purge_seq',
        'update_seq'
        ))

    expect_that(resnames,equals(api_names
        ))
    expect_that(res$db_name,equals(couch.makedbname.noescape(parts)))

})
couch.deletedb(parts)
