"use strict";

var assert = require('assert'),
    seneca = require('seneca'),
    async = require('async')


var shared = seneca.test.store.shared

var config = {
    dsn: 'hana',
    username: 'SYSTEM',
    password: 'manager',
//  db: 'SYSTEM',
    schema: 'hana_test',
    options: { }
}

var si = seneca({log:'print'})
si.use(require('..'), config)

si.__testcount = 0
var testcount = 0


describe('hana', function () {
  it('basic', function (done) {
    testcount++
    shared.basictest(si, done)
  })

  it('close', function (done) {
    shared.closetest(si, testcount, done)
  })
})