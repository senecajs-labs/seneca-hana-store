 'use strict'


 var lab = exports.lab = require('lab').script()
 var describe = lab.describe
 var it = lab.it
 var Util = require('../lib/relational-util')


 describe('HANA mapper tests', function () {
   var odate = new Date('2013-11-29T20:21:22.987')
   var sdate = '2013-11-29T20:21:22.987'
   var ldate = 1385756482987
   it('DATE mapper', function (done) {
     var d = Util.getMapper('DATE').toSQL(odate)
     console.log('to SQL: %s', d.value)
     d = Util.getMapper('DATE').toJS(d.value)
     console.log('to  JS: %s', d)
     d = Util.getMapper('DATE').toSQL(sdate)
     console.log('to SQL: %s', d.value)
     d = Util.getMapper('DATE').toJS(d.value)
     console.log('to  JS: %s', d)
     d = Util.getMapper('DATE').toSQL(ldate)
     console.log('to SQL: %s', d.value)
     d = Util.getMapper('DATE').toJS(d.value)
     console.log('to  JS: %s', d)
     done()
   })

   it('TIME mapper', function (done) {
     var d = Util.getMapper('TIME').toSQL(odate)
     console.log('to SQL: %s', d.value)
     d = Util.getMapper('TIME').toJS(d.value)
     console.log('to  JS: %s', d)
     d = Util.getMapper('TIME').toSQL(sdate)
     console.log('to SQL: %s', d.value)
     d = Util.getMapper('TIME').toJS(d.value)
     console.log('to  JS: %s', d)
     d = Util.getMapper('TIME').toSQL(ldate)
     console.log('to SQL: %s', d.value)
     d = Util.getMapper('TIME').toJS(d.value)
     console.log('to  JS: %s', d)

     done()
   })

   it('SECONDDATE mapper', function (done) {
     var d = Util.getMapper('SECONDDATE').toSQL(odate)
     console.log('to SQL: %s', d.value)
     d = Util.getMapper('SECONDDATE').toJS(d.value)
     console.log('to  JS: %s', d)
     d = Util.getMapper('SECONDDATE').toSQL(sdate)
     console.log('to SQL: %s', d.value)
     d = Util.getMapper('SECONDDATE').toJS(d.value)
     console.log('to  JS: %s', d)
     d = Util.getMapper('SECONDDATE').toSQL(ldate)
     console.log('to SQL: %s', d.value)
     d = Util.getMapper('SECONDDATE').toJS(d.value)
     console.log('to  JS: %s', d)
     done()
   })

   it('TIMESTAMP mapper', function (done) {
     var d = Util.getMapper('TIMESTAMP').toSQL(odate)
     console.log('to SQL: %s', d.value)
     d = Util.getMapper('TIMESTAMP').toJS(d.value)
     console.log('to  JS: %s', d)
     d = Util.getMapper('TIMESTAMP').toSQL(sdate)
     console.log('to SQL: %s', d.value)
     d = Util.getMapper('TIMESTAMP').toJS(d.value)
     console.log('to  JS: %s', d)
     d = Util.getMapper('TIMESTAMP').toSQL(ldate)
     console.log('to SQL: %s', d.value)
     d = Util.getMapper('TIMESTAMP').toJS(d.value)
     console.log('to  JS: %s', d)

     done()
   })
 })
