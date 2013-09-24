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

  var schema = opts.schema || ''

  opts.minwait = opts.minwait || MIN_WAIT
  opts.maxwait = opts.maxwait || MAX_WAIT

  var minwait
  var dbinst = null
  var specifications = null
  var tablesmap = {}


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
//      inittblmap(dbinst, schema, function (err, map) {
//        for (var p in map) collmap[p] = map[p]
//        cb()
//      })
      cb()
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

      gettable(schema, ent, function (err, tblspec) {
        if (err) return cb(err)

        if (update) {
          dbinst.query(quoteprop(schema), updatestm(ent, tblspec), function (err, res) {
            if (!error(args, err, cb)) {
              seneca.log(args.tag$, 'update', ent)
              ent.load$(function (err, dbent) {
                if (err) return cb(err)
                return cb(null, dbent)
              })
            }
          })
        }
        else {
          getid(ent, tblspec, function (err, id) {
            if (!id) return cb(new Error('Error getting "id" value'))
            ent.id = id
            dbinst.query(quoteprop(schema), savestm(ent, tblspec), function (err, res) {
              if (!error(args, err, cb)) {
                seneca.log(args.tag$, 'save/insert', ent)
                ent.load$(function (err, dbent) {
                  if (err) return cb(err)
                  return cb(null, dbent)
                })
              }
            })
          })
        }
      })

    },


    load: function (args, cb) {
      var qent = args.qent
      var q = args.q

      gettable(schema, qent, function (err, tblspec) {
        if (err)return cb(err)

        dbinst.query(quoteprop(schema), selectstm(qent, q, tblspec), function (err, res) {
          if (!error(args, err, cb)) {
            var dbent = relationalstore.makeent(qent, res.rows[0], tblspec)
            seneca.log(args.tag$, 'load', res.rows[0])
            return cb(null, dbent)
          }
        })
      })
    },


    list: function (args, cb) {
      var qent = args.qent
      var q = args.q
      var list = []

      gettable(schema, qent, function (err, tblspec) {
        if (err)return cb(err)

        var query = selectstm(qent, q, tblspec)
        dbinst.query(quoteprop(schema), query, function (err, res) {
          if (!error(args, err, cb)) {
            res.rows.forEach(function (row) {
              var ent = relationalstore.makeent(qent, row, tblspec)
              list.push(ent)
            })
            seneca.log(args.tag$, 'list', list.length, list[0])
            return cb(null, list)
          }
        })
      })
    },


    remove: function (args, cb) {
      var qent = args.qent
      var q = args.q

      gettable(schema, qent, function (err, tblspec) {
        if (err)return cb(err)

        if (q.all$) {
          var query = deletestm(qent, q, tblspec)
          dbinst.query(quoteprop(schema), query, function (err, res) {
            if (!error(args, err, cb)) {
              seneca.log(args.tag$, 'remove', qent)
              return cb()
            }
          })
        }
        else {
          var select = selectstm(qent, q, tblspec)
          dbinst.query(quoteprop(schema), select, function (err, res) {
            if (!error(args, err, cb)) {
              var entp = res.rows[0]
              var query = deletestm(qent, entp, tblspec)
              dbinst.query(quoteprop(schema), query, function (err, res) {
                if (!error(args, err, cb)) {
                  seneca.log(args.tag$, 'remove', qent)
                  return cb()
                }
              })
            }
          })
        }
      })
    },


    native: function (args, cb) {
      cb(null, dbinst)
    }

  }


  function gettable(schema, ent, cb) {
    var canon = ent.canon$({object: true})
    var table = (canon.base ? canon.base + '_' : '') + canon.name

    if (!tablesmap[table]) {
      dbinst.db.describe({database: schema, schema: schema, table: table}, function (err, data) {
        if (err) {
          return cb(err)
        } else {
          var meta = tblmeta(data)
          if (_.isNull(meta) || _.isUndefined(meta)) {
            return cb(new Error('Metadata not found for table: ' + table))
          } else {
            tablesmap[table] = meta
            return cb(null, meta)
          }
        }
      })
    } else {
      return cb(null, tablesmap[table])
    }
  }


  function getid(ent, tblspec, cb) {

    if(ent.id$) {
      return cb(null, ent.id$)
    } else if (opts.idtype && 'sequence' === opts.idtype) {
      dbinst.query(quoteprop(tblspec.schemaName), idvalstm(tblspec.tableName), function (err, data) {
        if (err) return cb(err)
        var hasData = data && data.rows && data.rows[0]
        if (hasData) {
          for (var p in data.rows[0]) {
            var id = data.rows[0][p]
          }
          return cb(null, id)
        } else {
          return cb(null, null)
        }
      })
    } else {
      return cb(null, uuid.v4())
    }
  }


  var meta = seneca.store.init(seneca, opts, store)

  desc = meta.desc

  seneca.add({init: store.name, tag: meta.tag}, function (args, done) {
    configure(opts, function (err) {
      if (err) {
        console.log(err.nane, err.message, err.stack)
        return seneca.fail({code: 'entity/configure', store: store.name, error: err, desc: desc}, done)
      } else {
        done()
      }
    })
  })

  return {name: store.name, tag: meta.tag}
}


function savestm(ent, tblspec) {

  var table = tblspec.tableName,
      entp = relationalstore.makeentp(ent, tblspec),
      fields = _.keys(entp)

  var cols = []
  var vals = []

  fields.forEach(function (field) {
    cols.push(quoteprop(field))
    vals.push(quoteval(entp[field]))
  })

  return 'INSERT INTO ' + quoteprop(table) + ' (' + cols + ') VALUES (' + vals + ')'
}


function updatestm(ent, tblspec) {
  var table = tblspec.tableName,
      fields = ent.fields$(),
      entp = relationalstore.makeentp(ent, tblspec)

  var params = []

  fields.forEach(function (field) {
    if (!(_.isUndefined(ent[field]) || _.isNull(ent[field]))) {
      params.push(quoteprop(field) + ' = ' + quoteval(entp[field]))
    }
  })

  return 'UPDATE ' + quoteprop(table) + ' SET ' + params + ' WHERE "id" = ' + quoteval(ent.id)
}


function deletestm(qent, q, tblspec) {

  var table = tblspec.tableName,
      entp = relationalstore.makeentp(qent, tblspec)

  return 'DELETE FROM ' + quoteprop(table) + wherestm(entp, q, tblspec.columns)
}


function selectstm(qent, q, tblspec) {
  var table = tblspec.tableName,
      entp = relationalstore.makeentp(qent, tblspec)

  return 'SELECT * FROM ' + quoteprop(table) + wherestm(entp, q, tblspec.columns) + metastm(q)
}


function wherestm(entp, q, columns) {
  var qok = relationalstore.fixquery(entp, q)

  var wargs = []
  for (var p in qok) {
    if (qok[p] && relationalstore.isAllowed(columns[p].dataTypeName)) {
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
    if (q.skip$) {
      params.push('OFFSET ' + q.skip$)
    }
  }

  return ' ' + params.join(' ')
}


function quoteprop(val) {
  return '"' + val + '"'
}


function quoteval(val) {
  return '\'' + escapeSQL(val) + '\''
}


function escapeSQL(input) {
  var str = "" + input;
  return str.replace(/[']/g, function (char) {
    switch (char) {
      case "'":
        return "''";
    }
  })
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


function idvalstm(tableName) {
  var seqName = tableName + '_id'
  return 'SELECT "' + seqName + '".NEXTVAL from DUMMY'
}


function tblmeta(data) {
  if (_.isNull(data) || _.isUndefined(data)) {
    return null
  } else {
    var tblmeta = {}
    tblmeta.columns = {}
    data.forEach(function (meta) {
      if (meta.COLUMN_NAME) {
        tblmeta.schemaName = meta.TABLE_SCHEM
        tblmeta.tableName = meta.TABLE_NAME
        tblmeta.columns[meta.COLUMN_NAME] = {
          columnName: meta.COLUMN_NAME,
          dataType: meta.DATA_TYPE,
          dataTypeName: meta.TYPE_NAME,
          length: meta.COLUMN_SIZE,
          isNullable: meta.NULLABLE == 0 ? false : true
        }
      } else {
        console.error("Empty column name: %s", meta);
      }
    })
    return tblmeta
  }
}

