library('rcouchutils')
pkg <- as.package('.')
Sys.setenv(RCOUCHDBUTILS_CONFIG_FILE=paste(pkg$path,'test.config.json',sep='/'))

sourceparts <- c('replication','testing','a')
destparts <- c('replication','testing','b')
destparts2 <- c('replication','testing','c')

couch.makedb(sourceparts)
couch.makedb(destparts)

test_that("can replicate a database on one machine",{

    source_dbname <-  couch.makedbname(sourceparts)
    h <-  RCurl::getCurlHandle()
    couch.session(h)

    ## populate source with some data
    doc <- list()
    doc[['one potato']] <- 'two potatoes'
    doc$beer <- 'food group'
    doc$foo <- 123

    someids <- c('saute','eggs','bacon','sausage')

    for(id in someids){
        couch.put(sourceparts,
                  docname=id,
                  doc=doc,
                  h=h)
    }
    res <- couch.attach(sourceparts,someids[1],'./files/fig.png',h=h)
    expect_that(res,is_a('list'))
    expect_that(res$ok,equals(TRUE))

    src_status <- get.db(sourceparts,h=h)

    ## now replicate
    rep1 <- couch.start.replication(src=couch.makedbname.noescape(sourceparts),
                                    tgt=couch.makedbname.noescape(destparts)
                                    ,id='testreplication1'
                                    )

    expect_that(rep1$ok,equals(TRUE))
    print('sleep for 2')
    Sys.sleep(2) ## way more than necessary

    print('wake, check status')
    ## is the target replication triggered?
    dst_status <- get.db(destparts,h=h)
    expect_that(dst_status$doc_count,equals(src_status$doc_count))

    print('check content')
    copies <- couch.allDocs(destparts)
    for(row in copies$rows){
        cdoc <-  row$doc
        expect_that(cdoc[['one potato']],equals('two potatoes'))
        expect_that(cdoc$beer,equals('food group'))
        expect_that(cdoc$foo,equals(123))
        if(id == someids[1]){
            ## should have the attachment
            expect_that(cdoc[['_attachments']],has_names(c('fig.png')))
        }
    }



    ## and another, this time creating
    rep2 <- couch.start.replication(src=couch.makedbname.noescape(sourceparts),
                                    tgt=couch.makedbname.noescape(destparts2),
                                    create_target=TRUE,
                                    continuous=TRUE
                                    ,id='testreplication2'
                                    )

    expect_that(rep2$ok,equals(TRUE))
    expect_that('error' %in% names(rep2),equals(FALSE))

    if(!is.null(rep2$error)){
        print(rep2)
        print('bailing out of replication test early')
        stop()
    }

    print('sleep for 1')

    Sys.sleep(1)

    print('wake, check status')

    dst_status <- get.db(destparts2,h=h)
    expect_that(dst_status$doc_count,equals(src_status$doc_count))

    print('check the copy docs')
    copies <- couch.allDocs(destparts2)
    for(row in copies$rows){
        cdoc <-  row$doc
        expect_that(cdoc[['one potato']],equals('two potatoes'))
        expect_that(cdoc$beer,equals('food group'))
        expect_that(cdoc$foo,equals(123))
        if(id == someids[1]){
            ## should have the attachment
            expect_that(cdoc[['_attachments']],has_names(c('fig.png')))
        }
    }


    ## now put something in a, and you should see it in c with
    ## continuous replication, but it should not be in b

    newdoc <- list()
    newdoc$calendar <- 'crossword'
    newdoc$status <- rep2
    res <- couch.put(sourceparts,docname='anotherdoc',doc=newdoc)
    expect_that(res$ok,equals(TRUE))
    print('sleep for two')
    Sys.sleep(1)
    fetch <- couch.get(destparts2,'anotherdoc')
    expect_that(fetch$calendar,equals('crossword'))

    expect_that(fetch$status,equals(rep2))

})

couch.deletedb(sourceparts)
couch.deletedb(destparts)
couch.deletedb(destparts2)
