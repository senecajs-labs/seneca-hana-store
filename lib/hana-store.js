"use strict"

var _ = require('underscore'),
    util = require('util'),
    uuid = require('node-uuid'),
    hana = require('hana-odbc'),
    relationalstore = require('./relational-util')


var name = 'hana-store'

var MIN_WAIT = 16
var MAX_WAIT = 5000


module.exports = function (opts) {
  var seneca = this
  var desc
  var schema = (opts.schema) ? quoteprop(opts.schema) : ''

  opts.minwait = opts.minwait || MIN_WAIT
  opts.maxwait = opts.maxwait || MAX_WAIT

  var minwait
  var dbinst = null
  var specifications = null


  function error(args, err, cb) {
    if (err) {
      seneca.fail({code: 'entity/configure', store: store.name, error: err, desc: desc}, cb)

      if ('ECONNREFUSED' == err.code || 'notConnected' == err.message || 'Error: no open connections' == err) {
        if (minwait = opts.minwait) {
          reconnect(args)
        }
      }

      return true
    }

    return false
  }


  function reconnect(args) {
    seneca.log.debug('attempting db reconnect')

    configure(specifications, function (err) {
      if (err) {
        seneca.log.debug('db reconnect (wait ' + opts.minwait + 'ms) failed: ' + err)
        minwait = Math.min(2 * minwait, opts.maxwait)
        setTimeout(function () {
          reconnect(args)
        }, minwait)
      }
      else {
        minwait = opts.minwait
        seneca.log.debug('reconnect ok')
      }
    })
  }


  function configure(spec, cb) {
    specifications = spec

    var conf
    if (!conf) {
      conf = {}
      conf.dsn = spec.dsn
      conf.username = spec.username
      conf.password = spec.password
      conf.db = spec.db
      conf.schema = spec.schema || ''
    }

    var dsn = util.format('DSN=' + conf.dsn + ';UID=%s;PWD=%s', conf.username, conf.password)

    dbinst = hana.getSession({dsn: dsn})

    dbinst.connect(function (err) {
      if (err) return cb(err)
      return cb()
    });
  }


  var store = {
    name: name,

    close: function (cb) {
      if (dbinst) {
        dbinst.close()
      }
    },


    save: function (args, cb) {
      var ent = args.ent

      var update = !!ent.id;

      if (update) {
        var query = updatestm(ent)
        dbinst.query(schema, query, function (err, res) {
          if (!error(args, err, cb)) {
            seneca.log(args.tag$, 'update', res.rows)
            return cb(null, ent)
          }
        })
      }
      else {
        ent.id = uuid()
        var query = savestm(ent)
        dbinst.query(schema, query, function (err, res) {
          if (!error(args, err, cb)) {
            seneca.log(args.tag$, 'save', res.rows)
            return cb(null, ent)
          }
        })
      }
    },


    load: function (args, cb) {
      var qent = args.qent
      var q = args.q

      var query = selectstm(qent, q)
      dbinst.query(schema, query, function (err, res) {
        if (!error(args, err, cb)) {
          var dbent = relationalstore.makeent(qent, res.rows[0])
          seneca.log(args.tag$, 'load', res.rows[0])
          return cb(null, dbent)
        }
      })
    },


    list: function (args, cb) {
      var qent = args.qent
      var q = args.q
      var list = []

      var query = selectstm(qent, q)
      dbinst.query(schema, query, function (err, res) {
        if (!error(args, err, cb)) {
          res.rows.forEach(function (row) {
            var ent = relationalstore.makeent(qent, row)
            list.push(ent)
          })
          seneca.log(args.tag$, 'list', list.length, list[0])
          return cb(null, list)
        }
      })
    },


    remove: function (args, cb) {
      var qent = args.qent
      var q = args.q

      if (q.all$) {
        var query = deletestm(qent, q)
        dbinst.query(schema, query, function (err, res) {
          if (!error(args, err, cb)) {
            seneca.log(args.tag$, 'remove', res.rowCount)
            return cb(null, res.rowCount)
          }
        })
      }
      else {
        var select = selectstm(qent, q)
        dbinst.query(schema, select, function (err, res) {
          if (!error(args, err, cb)) {
            var entp = res.rows[0]
            var query = deletestm(qent, entp)
            dbinst.query(schema, query, function (err, res) {
              if (!error(args, err, cb)) {
                seneca.log(args.tag$, 'remove', res.rowCount)
                return cb(null, res.rowCount)
              }
            })
          }
        })
      }
    },


    native: function (args, done) {
//      dbinst.collection('seneca', function(err,coll){
//        if( !error(args,err,cb) ) {
//          coll.findOne({},{},function(err,entp){
//            if( !error(args,err,cb) ) {
//              done(null,dbinst)
//            }else{
//              done(err)
//            }
//          })
//        }else{
//          done(err)
//        }
//      })
    }

  }


  var meta = seneca.store.init(seneca, opts, store)

  desc = meta.desc

  seneca.add({init: store.name, tag: meta.tag}, function (args, done) {
    configure(opts, function (err) {
      if (err) {
        console.log(err.nane, err.message, err.stack)
        return seneca.fail({code: 'entity/configure', store: store.name, error: err, desc: desc}, done)
      }
      else done()
    })
  })


  return {name: store.name, tag: meta.tag}
}


function savestm(ent) {

  var table = relationalstore.tablename(ent)
  var entp = relationalstore.makeentp(ent)
  var fields = _.keys(entp)

  var cols = []
  var vals = []

  fields.forEach(function (field) {
    cols.push(quoteprop(field))
    vals.push(quoteval(entp[field]))
  })

  return 'INSERT INTO ' + quoteprop(table) + ' (' + cols + ') VALUES (' + vals + ')'
}


function updatestm(ent) {
  var table = relationalstore.tablename(ent),
      fields = ent.fields$(),
      entp = relationalstore.makeentp(ent)

  var params = []

  fields.forEach(function (field) {
    if (!(_.isUndefined(ent[field]) || _.isNull(ent[field]))) {
      params.push(quoteprop(field) + ' = ' + quoteval(entp[field]))
    }
  })

  return 'UPDATE ' + quoteprop(table) + ' SET ' + params + ' WHERE "id" = ' + quoteval(ent.id)
}


function deletestm(qent, q) {

  var table = relationalstore.tablename(qent),
      entp = relationalstore.makeentp(qent)

  return 'DELETE FROM ' + quoteprop(table) + wherestm(entp, q)
}


function selectstm(qent, q) {
  var table = relationalstore.tablename(qent),
      entp = relationalstore.makeentp(qent)

  return 'SELECT * FROM ' + quoteprop(table) + wherestm(entp, q) + metastm(q)
}


function wherestm(entp, q) {
  var qok = relationalstore.fixquery(entp, q)

  var wargs = []
  for (var p in qok) {
    if (qok[p]) {
      wargs.push(quoteprop(p) + ' = ' + quoteval(qok[p]))
    }
  }

  var stm = ''
  if (wargs && wargs.length > 0) {
    stm = ' WHERE ' + wargs.join(' AND ')
  }

  return stm
}


function metastm(q) {
  var params = []

  if (q.sort$) {
    for (var sf in q.sort$) break;
    var sd = q.sort$[sf] < 0 ? 'ASC' : 'DESC'
    params.push('ORDER BY ' + quoteprop(sf) + ' ' + sd)
  }

  if (q.limit$) {
    params.push('LIMIT ' + q.limit$)
  }

  return ' ' + params.join(' ')
}


function quoteprop(val) {
//  return '"' + escapeStr(val) + '"'
  return '"' + val + '"'
}


function quoteval(val) {
//  return '\'' + escapeStr(val) + '\''
  return '\'' + val + '\''
}


function escapeStr(input) {
  var str = "" + input;
  return str.replace(/[\0\b\t\x08\x09\x1a\n\r"'\\\%]/g, function (char) {
    switch (char) {
      case "\0":
        return "\\0";
      case "\x08":
        return "\\b";
      case "\b":
        return "\\b";
      case "\x09":
        return "\\t";
      case "\t":
        return "\\t";
      case "\x1a":
        return "\\z";
      case "\n":
        return "\\n";
      case "\r":
        return "\\r";
      case "\"":
      case "'":
      case "\\":
      case "%":
        return "\\" + char;

    }
  });
};
