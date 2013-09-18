"use strict";

var assert = require('assert'),
    seneca = require('seneca'),
    async = require('async')


var shared = seneca.test.store.shared


var options = {
    dsn: 'hana',
    username: 'SYSTEM',
    password: 'manager',
    schema: 'hana_test',
    idtype: 'sequence'
}

var si = seneca()
si.use(require('..'), options)

si.__testcount = 0
var testcount = 0

var cleanup_cmds = [
  'DELETE FROM "foo"',
  'DELETE FROM "moon_bar"'
]

function prepdb(dbi, cmds, cb) {
  function cmdit(query, callback) {
    console.log(query)
    dbi.query('\"' + options.schema + '\"', query, function (err, res) {
      console.log(res)
      callback(err, res)
    })
  }
  async.eachSeries(cmds, cmdit, cb)
}


describe('hana', function () {

  before(function (done) {
    si.ready(function () {
      var ent = si.make$('sys')
      ent.native$(function (err, dbi) {
        prepdb(dbi, cleanup_cmds, done)
      })
    })
  })


  it('basic', function (done) {
    testcount++
    shared.basictest(si, done)
  })


  it('close', function (done) {
    shared.closetest(si, testcount, done)
  })
})