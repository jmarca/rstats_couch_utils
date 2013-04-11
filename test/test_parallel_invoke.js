/* global require console process describe it */

var should = require('should')
//var setter = require('../couch_set_state')
//var getter = require('couch_check_state')

var _ = require('lodash')
var async = require('async')

var superagent = require('superagent')

var util  = require('util')
var spawn = require('child_process').spawn;
var path = require('path');
var fs = require('fs');

var pg = require('pg')



var R;


var env = process.env;
var cuser = env.COUCHDB_USER ;
var cpass = env.COUCHDB_PASS ;
var chost = env.COUCHDB_HOST || 'localhost';
var cport = env.COUCHDB_PORT || 5984;

var test_db ='test%2fcrashing%2fattachments'
var couch = 'http://'+chost+':'+cport+'/'+test_db

var docs = {'docs':[{'_id':'doc1'
                    ,foo:'bar'}
                   ,{'_id':'doc2'
                    ,'baz':'bat'}
                   ,{'_id':'doc3'
                    ,'key':'bored'}
                   ]}

describe('couch.get.attachment',function(){
    var created_locally=false
    before(function(done){
        // create a test db, the put data into it
        var opts = {'uri':couch
                   ,'method': "PUT"
                   ,'headers': {}
                   };
        opts.headers.authorization = 'Basic ' + new Buffer(cuser + ':' + cpass).toString('base64')
        opts.headers['Content-Type'] =  'application/json'
        superagent.put(couch)
        .auth(cuser,cpass)
        .end(function(e,r){
            r.should.have.property('error',false)
            if(!e)
                created_locally=true
            // now populate that db with some docs
            superagent.post(couch+'/_bulk_docs')
            .type('json')
            .set('Accept', 'application/json')
            .send(docs)
            .end(function(e,r){
                if(e) done(e)
                docs=[]
                _.each(r.body
                      ,function(resp){
                           resp.should.have.property('ok')
                           docs.push({doc:{_id:resp.id
                                          ,_rev:resp.rev}})
                       });
                // now attach file to each
                async.eachSeries(docs,function(d,attdone){
                    var stream = fs.createReadStream('test/files/wim.31.S.vdsid.400350.2007.paired.RData')
                    var doc = d.doc
                    var uri = [couch,doc._id,'attach'+doc._id].join('/')
                    uri += '?rev='+doc._rev
                    var req = superagent.put(uri)
                    req.type("application/x-binary")
                    req.set('Accept', 'application/json')

                    req.on('response', function(res){
                        res.status.should.eql(201);
                        var rev = res.body.rev
                        doc._rev = rev
                        return attdone()
                    });
                    stream.pipe(req)

                },done)
                return null
            })
            return null
        })
    })
    after(function(done){
        if(!created_locally) return done()

        var couch = 'http://'+chost+':'+cport+'/'+test_db
        // bail in development
        //console.log(couch)
        //return done()
        var opts = {'uri':couch
                   ,'method': "DELETE"
                   ,'headers': {}
                   };
        superagent.del(couch)
        .type('json')
        .auth(cuser,cpass)
        .end(function(e,r){
            if(e) return done(e)
            return done()
        })
        return null
    })

    it('should invoke multiple R jobs that do not crash getting attachments'
      ,function(done){

           var RCall = ['--no-restore','--no-save','test/att_load.R']
           function spawnR(task,cb){
               // trigger the file loop callback
               var doc = task.doc
               var opts = _.clone(task.opts)
               opts.env['RDOCFILE']=doc
               var R  = spawn('Rscript', RCall, opts);
               R.stderr.setEncoding('utf8')
               R.stdout.setEncoding('utf8')
               var logfile = 'r_'+task.id+'.log'
               var log = fs.createWriteStream(logfile
                                             ,{flags: 'a',encoding: 'utf8',mode: 0666 })
               R.stdout.pipe(log)
               R.stderr.pipe(log)
               R.on('exit',function(code){
                   console.log('got exit: '+code+', for ',task.id)
                   code.should.eql(10)
                   cb()
               })
           }

           var opts = { cwd: undefined,
                        env: process.env
                      }
           opts.env.RTESTDB=test_db
           // fire off 3 jobs
           var jq=async.queue(spawnR,10)
           // assign a callback
           jq.drain = function() {
               console.log('all items have been processed');
               done()
           }
           var jobid = 0
           for (var i=0;i<10;i++){
               async.each(docs,function(d,acb){
                   var doc = docs[0].doc
                   jq.push({'opts':_.clone(opts)
                           ,'id':jobid++
                           ,'doc':doc._id
                           },acb)
               })
           }

       });

})
