"use strict";

var assert = require('assert'),
    seneca = require('seneca'),
    async = require('async')

var util = require('../lib/relational-util')

describe('HANA mapper tests', function () {
  var odate = new Date('2013-11-29T20:21:22.987')
  var sdate = '2013-11-29T20:21:22.987'
  var ldate = 1385756482987

  it('DATE mapper', function (done) {
    var d = util.getMapper('DATE').toSQL(odate)
    console.log('to SQL: %s',d.value)
    var d = util.getMapper('DATE').toJS(d.value)
    console.log('to  JS: %s',d)
    var d = util.getMapper('DATE').toSQL(sdate)
    console.log('to SQL: %s',d.value)
    var d = util.getMapper('DATE').toJS(d.value)
    console.log('to  JS: %s',d)
    var d = util.getMapper('DATE').toSQL(ldate)
    console.log('to SQL: %s',d.value)
    var d = util.getMapper('DATE').toJS(d.value)
    console.log('to  JS: %s',d)
    done()
  })

  it('TIME mapper', function (done) {
    var d = util.getMapper('TIME').toSQL(odate)
    console.log('to SQL: %s',d.value)
    var d = util.getMapper('TIME').toJS(d.value)
    console.log('to  JS: %s',d)
    var d = util.getMapper('TIME').toSQL(sdate)
    console.log('to SQL: %s',d.value)
    var d = util.getMapper('TIME').toJS(d.value)
    console.log('to  JS: %s',d)
    var d = util.getMapper('TIME').toSQL(ldate)
    console.log('to SQL: %s',d.value)
    var d = util.getMapper('TIME').toJS(d.value)
    console.log('to  JS: %s',d)

    done()
  })

  it('SECONDDATE mapper', function (done) {
    var d = util.getMapper('SECONDDATE').toSQL(odate)
    console.log('to SQL: %s',d.value)
    var d = util.getMapper('SECONDDATE').toJS(d.value)
    console.log('to  JS: %s',d)
    var d = util.getMapper('SECONDDATE').toSQL(sdate)
    console.log('to SQL: %s',d.value)
    var d = util.getMapper('SECONDDATE').toJS(d.value)
    console.log('to  JS: %s',d)
    var d = util.getMapper('SECONDDATE').toSQL(ldate)
    console.log('to SQL: %s',d.value)
    var d = util.getMapper('SECONDDATE').toJS(d.value)
    console.log('to  JS: %s',d)
    done()
  })

  it('TIMESTAMP mapper', function (done) {
    var d = util.getMapper('TIMESTAMP').toSQL(odate)
    console.log('to SQL: %s',d.value)
    var d = util.getMapper('TIMESTAMP').toJS(d.value)
    console.log('to  JS: %s',d)
    var d = util.getMapper('TIMESTAMP').toSQL(sdate)
    console.log('to SQL: %s',d.value)
    var d = util.getMapper('TIMESTAMP').toJS(d.value)
    console.log('to  JS: %s',d)
    var d = util.getMapper('TIMESTAMP').toSQL(ldate)
    console.log('to SQL: %s',d.value)
    var d = util.getMapper('TIMESTAMP').toJS(d.value)
    console.log('to  JS: %s',d)

    done()
  })

})
