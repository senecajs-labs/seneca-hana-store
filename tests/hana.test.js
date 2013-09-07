"use strict";

var assert = require('assert'),
    seneca = require('seneca'),
    async = require('async')


var shared = seneca.test.store.shared


var options = {
    dsn: 'hana',
    username: 'SYSTEM',
    password: 'manager',
    schema: 'hana_test'
}

var si = seneca()
si.use(require('..'), options)

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