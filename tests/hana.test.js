
'use strict'

var Seneca = require('seneca')
var Shared = require('seneca-store-test')

var lab = exports.lab = require('lab').script()

var options = {
  host: 'imdbhdb',
  port: 30015,
  user: 'system',
  password: 'manager',
  schema: 'hana_test'
}

var si = Seneca({log: 'print'})
si.use(require('..'), options)

var describe = lab.describe
describe('hana', function () {
  Shared.basictest({
    seneca: si,
    script: lab
  })
})
