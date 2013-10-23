"use strict";

var assert = require('assert'),
  seneca = require('seneca'),
  async = require('async');


var shared = seneca.test.store.shared;


var options = {
  dsn: 'hana',
  username: 'SYSTEM',
  password: 'manager',
  schema: 'hana_test',
  idtype: 'sequence'
};

var si = seneca({log: 'print'});
si.use(require('..'), options);

si.__testcount = 0;
var testcount = 0;

var cleanup_cmds = [
  'DELETE FROM "foo"',
  'DELETE FROM "moon_bar"'
];

function prepdb(dbi, cmds, cb) {
  function cmdit(query, callback) {
    dbi.query('\"' + options.schema + '\"', query, function (err, res) {
      callback(err, res);
    });
  }

  async.eachSeries(cmds, cmdit, cb);
}


describe('hana', function () {

  before(function (done) {
    si.ready(function () {
      var ent = si.make$('sys');
      ent.native$(function (err, dbi) {
        prepdb(dbi, cleanup_cmds, done);
      });
    });
  });


  it('basic', function (done) {
    testcount++
    shared.basictest(si, done);
  });

//
//  it.skip('native-odbc', function (done) {
//    var ent = si.make('test', {name: 'name', address: 'address'})
//    var dent = ent.data$(false)
//    var ent = si.make$('sys')
//    ent.native$(function (err, dbi) {
//      var db = dbi.db
//
////      db.prepare('SELECT * from "egalio"."sys_user"', function (err,stm){
////        if(err) return cb(err)
////        stm.execute(function(err,res){
////          if(err) return cb(err)
//////          console.log(sys.inspect(res))
////          console.log(res)
////          res.fetchAll(function(err,res){
////            console.log(res)
////          })
////        })
////      })
////      // describe database
////      db.describe({
////        database: 'hana_test',schema: 'hana_test'
////      }, function (err, data) {
////        if (err) {
////          console.error(err);
////          process.exit(1);
////        }
////        console.dir(data);
////        done()
////      });
//
//      // describe table
//      var schema = 'hana_test'
//      var table = 'foo'
//      db.describe({
//        database: schema, schema: schema,
//        table: table
//      }, function (err, data) {
//        if (err) {
//          console.error(err);
//          process.exit(1);
//        }
//        console.error(data);
//        done()
//      });
//
//      // describe column
////      db.describe({
////        database : 'hana_test',
////        table : 'foo',
////        column : 'id'
////      }, function (err, data) {
////        if (err) {
////          console.error(err);
////          process.exit(1);
////        }
////        console.error(data);
////      });
//    })
//  })
//
  it('close', function (done) {
    shared.closetest(si, testcount, done);
  });

});

