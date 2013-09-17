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

  var opts = opts
  var schema = opts.schema || ''
  var idtype = opts.idtype

  opts.minwait = opts.minwait || MIN_WAIT
  opts.maxwait = opts.maxwait || MAX_WAIT

  var minwait
  var dbinst = null
  var specifications = null
  var collmap = {}


  function idstr(ent) {
    if (idtype && 'sequence' === idtype) {
      return '"' + relationalstore.tablename(obj) + '_id".NEXTVAL'
    } else {
      return uuid.v4()
    }
  }


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
      inittblmap(dbinst, schema, cb)
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
        dbinst.query(quoteprop(schema), updatestm(ent), function (err, res) {
          if (!error(args, err, cb)) {
            seneca.log(args.tag$, 'update', res.rows)
            return cb(null, ent)
          }
        })
      }
      else {
        dbinst.query(quoteprop(schema), idvalstm(ent), function (err, data) {
          if (!error(args, err, cb)) {
            var id = getid(data)
            if (!id) return cb(new Error('Error retrieving sequence value'))
            ent.id = id
            dbinst.query(quoteprop(schema), savestm(ent), function (err, res) {
              seneca.log(args.tag$, 'save/insert', ent)
              if (!error(args, err, cb)) {
                cb(null, ent)
              }
            })
          }
        })
      }
    },


    load: function (args, cb) {
      var qent = args.qent
      var q = args.q
      getcoll(args, qent, function (err, res) {
        dbinst.query(quoteprop(schema), selectstm(qent, q), function (err, res) {
          if (!error(args, err, cb)) {
            var dbent = relationalstore.makeent(qent, res.rows[0])
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

      var query = selectstm(qent, q)
      dbinst.query(quoteprop(schema), query, function (err, res) {
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
        dbinst.query(quoteprop(schema), query, function (err, res) {
          if (!error(args, err, cb)) {
            seneca.log(args.tag$, 'remove', res.rowCount)
            return cb(null, res.rowCount)
          }
        })
      }
      else {
        var select = selectstm(qent, q)
        dbinst.query(quoteprop(schema), select, function (err, res) {
          if (!error(args, err, cb)) {
            var entp = res.rows[0]
            var query = deletestm(qent, entp)
            dbinst.query(quoteprop(schema), query, function (err, res) {
              if (!error(args, err, cb)) {
                seneca.log(args.tag$, 'remove', res.rowCount)
                return cb(null, res.rowCount)
              }
            })
          }
        })
      }
    },


    native: function (args, cb) {
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


  function getcoll(args, ent, cb) {
    var canon = ent.canon$({object: true})

    var collname = (canon.base ? canon.base + '_' : '') + canon.name

    cb(null, collmap[collname])
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


function idvalstm(ent) {
  var sequence = relationalstore.tablename(ent) + '_id'
  return 'SELECT "' + sequence + '".NEXTVAL from DUMMY'
}


function getid(data) {
  var hasData = data && data.rows && data.rows[0]
  if (hasData) {
    for (var p in data.rows[0]) {
      return data.rows[0][p]
    }
  } else {
    return null
  }
}


function inittblmap(dbinst, schema, cb) {
  var collmap = {}

  var query = 'SELECT SCHEMA_NAME,TABLE_NAME,TABLE_TYPE FROM "SYS"."TABLES" WHERE "SCHEMA_NAME" = ' + quoteval(schema)
  dbinst.query("SYS", query, function (err, tbldata) {
    if (err) return cb(err)
    if (tbldata.rows) {
      tbldata.rows.forEach(function (row) {
        collmap[row['TABLE_NAME']] = {
          schema_name: row['SCHEMA_NAME'],
          table_name: row['TABLE_NAME'],
          table_type: row['TABLE_TYPE'],
          columns: {}
        }
      })
    }

    query = 'SELECT SCHEMA_NAME,TABLE_NAME,COLUMN_NAME,DATA_TYPE_ID,DATA_TYPE_NAME,LENGTH,SCALE,IS_NULLABLE,CS_DATA_TYPE_ID,CS_DATA_TYPE_NAME FROM "SYS"."COLUMNS" WHERE "SCHEMA_NAME" = ' + quoteval(schema)
    dbinst.query("SYS", query, function (err, coldata) {
      if (err) return cb(err)
      if (coldata.rows) {
        coldata.rows.forEach(function (row) {
          var tblname = row['TABLE_NAME']
          if (!collmap[tblname]) {
            collmap[tblname] = {
              schema_name: row['SCHEMA_NAME'],
              table_name: row['TABLE_NAME'],
              columns: {}
            }
          }
          collmap[tblname].columns[row['COLUMN_NAME']] = {
            column_name: row['COLUMN_NAME'],
            data_type_id: row['DATA_TYPE_ID'],
            data_type_name: row['DATA_TYPE_NAME'],
            cs_data_type_id: row['CS_DATA_TYPE_ID'],
            cs_data_type_name: row['CS_DATA_TYPE_NAME'],
            length: row['LENGTH'],
            scale: row['SCALE'],
            is_nullable: row['IS_NULLABLE']
          }
        })
        cb(null, collmap)
      }
    })
  })
}

function map() {
//  TIMESTAMP
//  SECONDDATE
//  DATE
//  TIME
//  DOUBLE
//  REAL
//  DECIMAL
//  SMALLDECIMAL
//  BIGINT
//  INTEGER
//  SMALLINT
//  TINYINT
//  NCLOB
//  NVARCHAR
//  CLOB
//  VARCHAR
//  BLOB
//  VARBINARY
}