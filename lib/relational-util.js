/*jslint node: true*/
/*jslint asi: true */
/* Base class for relational databases */
"use strict";

var _ = require('underscore')
var moment = require('moment')

var SENECA_TYPE_COLUMN = 'seneca'

var BOOLEAN_TYPE = 'b'
var OBJECT_TYPE = 'o'
var ARRAY_TYPE = 'a'
var DATE_TYPE = 'd'


module.exports.fixquery = function (entp, q) {
  var qq = {};

  for (var qp in q) {
    if (!qp.match(/\$$/)) {
      qq[qp] = q[qp]
    }
  }

  if (_.isFunction(qq.id)) {
    delete qq.id
  }

  return qq
}


/**
 * Create a new persistable entity from the entity object. The function adds
 * the value for SENECA_TYPE_COLUMN with hints for type of the serialized objects.
 *
 * @param ent entity
 * @return {Object}
 */
module.exports.makeentp = function (ent, tblspec) {
  var entp = {}
  var type = {}

  if (!ent) return null

  var fields = ent.fields$()
  fields.forEach(function (field) {
    var cspec = tblspec.columns[field]
    var dataType = (cspec && cspec.dataTypeName) ? cspec.dataTypeName : null
    var mappedval = getMapper(dataType).toSQL(ent[field])
    entp[field] = mappedval.value
    if (mappedval.type) {
      type[field] = mappedval.type
    }
  })

  if (!_.isEmpty(type)) {
    entp[SENECA_TYPE_COLUMN] = JSON.stringify(type)
  }

  return entp
}


/**
 * Create a new entity using a row from database. This function is using type
 * hints from database column SENECA_TYPE_COLUMN to deserialize stored values
 * into proper objects.
 *
 * @param ent entity
 * @param row database row data
 * @return {Entity}
 */
module.exports.makeent = function (ent, row, tblspec) {
  var entp = {}

  if (_.isUndefined(ent) || _.isUndefined(row)) {
    return null
  } else {
    var fields = _.keys(row)
    var hints = row[SENECA_TYPE_COLUMN]
    var senecatype = (hints) ? JSON.parse(hints) : {}

    fields.forEach(function (field) {
      var cspec = tblspec.columns[field]
      var dataType = (cspec && cspec.dataTypeName) ? cspec.dataTypeName : null
      if (SENECA_TYPE_COLUMN != field) {
        var value = getMapper(dataType).toJS(row[field], senecatype[field])
        entp[field] = value
      }
    })
  }

  return ent.make$(entp)
}


module.exports.tablename = function (entity) {
  var canon = entity.canon$({object: true})

  var name = (canon.base ? canon.base + '_' : '') + canon.name

  return name
}


var mapper = {
  DEFAULT: {
    toSQL: function (val) {
      return {value: val}
    },
    toJS: function (val) {
      return val
    }
  },
  TIMESTAMP: {
    toSQL: function (val) {
      var value
      if (val && moment.utc(val).isValid()) {
        value = moment.utc(val).format('YYYY-MM-DD HH:mm:ss.SSS')
      } else {
        value = null
      }
      return {value: value}
    },
    toJS: function (val) {
      var value
      if (val && moment.utc(val).isValid()) {
        value = moment.utc(val, 'YYYY-MM-DD HH:mm:ss.SSS').format('YYYY-MM-DD HH:mm:ss.SSS')
      } else {
        value = null
      }
      return value
    }
  },
  SECONDDATE: {
    toSQL: function (val) {
      var value
      if (val && moment.utc(val).isValid()) {
        value = moment.utc(val).format('YYYY-MM-DD HH:mm:ss')
      } else {
        value = null
      }
      return {value: value}
    },
    toJS: function (val) {
      var value
      if (val && moment.utc(val).isValid()) {
        value = moment.utc(val, 'YYYY-MM-DD HH:mm:ss').format('YYYY-MM-DD HH:mm:ss')
      } else {
        value = null
      }
      return value
    }
  },
  DATE: {
    toSQL: function (val) {
      var value
      if (val && moment.utc(val).isValid()) {
        value = moment.utc(val).format('YYYY-MM-DD')
      } else {
        value = null
      }
      return {value: value}
    },
    toJS: function (val) {
      var value
      if (val && moment.utc(val).isValid()) {
        value = moment.utc(val, 'YYYY-MM-DD').format('YYYY-MM-DD')
      } else {
        value = null
      }
      return value
    }
  },
  TIME: {
    toSQL: function (val) {
      var value
      if (val && moment.utc(val).isValid()) {
        value = moment.utc(val).format('HH:mm:ss')
      } else {
        value = null
      }
      return {value: value}
    },
    toJS: function (val) {
      var value
      if (val) {
        value = moment(val, 'HH:mm:ss').format('HH:mm:ss')
      } else {
        value = null
      }
      return value
    }
  },
  STRING: {
    toSQL: function (val) {
      var map = {}
      if (_.isBoolean(val)) {
        map.value = JSON.stringify(val)
        map.type = BOOLEAN_TYPE
      }
      if (_.isDate(val)) {
        map.value = val.toISOString()
        map.type = DATE_TYPE
      }
      else if (_.isArray(val)) {
        map.value = JSON.stringify(val)
        map.type = ARRAY_TYPE
      }
      else if (_.isObject(val)) {
        map.value = JSON.stringify(val)
        map.type = OBJECT_TYPE;
      } else {
        map.value = val
      }
      return map
    },
    toJS: function (val, typeHint) {
      var value = val
      if (OBJECT_TYPE === typeHint) {
        value = JSON.parse(val)
      }
      else if (ARRAY_TYPE === typeHint) {
        value = JSON.parse(val)
      }
      else if (DATE_TYPE === typeHint) {
        value = new Date(val)
      }
      else if (BOOLEAN_TYPE === typeHint) {
        value = JSON.parse(val)
      }
      return value
    }
  }
}

/**
 * SAP HANA Data Types Reference: http://help.sap.com/hana/html/_csql_data_types.html
 *
 * Character string types  VARCHAR, NVARCHAR, ALPHANUM, SHORTTEXT
 * Datetime types  DATE, TIME, SECONDDATE, TIMESTAMP
 * Numeric types  TINYINT, SMALLINT, INTEGER, BIGINT, SMALLDECIMAL, DECIMAL, REAL, DOUBLE
 * Binary types  VARBINARY
 * Large Object types  BLOB, CLOB, NCLOB, TEXT
 *
 * @param dataType data type name
 * @returns {*}
 */
module.exports.getMapper = getMapper
function getMapper(dataType) {
  switch (dataType) {
    case 'VARCHAR':
    case 'NVARCHAR':
    case 'ALPHANUM':
    case 'SHORTTEXT':
      return mapper['STRING']
      break
    case 'DATE':
    case 'TIME':
    case 'SECONDDATE':
    case 'TIMESTAMP':
      return mapper[dataType]
      break
    case 'TINYINT':
    case 'SMALLINT':
    case 'INTEGER':
    case 'BIGINT':
    case 'SMALLDECIMAL':
    case 'DECIMAL':
    case 'REAL':
    case 'DOUBLE':
    case 'VARBINARY':
    case 'BLOB':
    case 'CLOB':
    case 'NCLOB':
    case 'TEXT':
      return mapper['DEFAULT']
    default:
      console.log('Type %s not mapped. Using default mapper.', dataType)
      return mapper['DEFAULT']
  }
}


var CHARACTER_TYPES = ['VARCHAR', 'NVARCHAR', 'ALPHANUM', 'SHORTTEXT']
var DATETIME_TYPES = ['DATE', 'TIME', 'SECONDDATE', 'TIMESTAMP']
var NUMERIC_TYPES = ['TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'SMALLDECIMAL', 'DECIMAL', 'REAL', 'DOUBLE']
var BINARY_TYPES = ['VARBINARY']
var LOB_TYPES = ['BLOB', 'CLOB', 'NCLOB', 'TEXT']
// types allowed in where clause
var ALLOWED = _.union(CHARACTER_TYPES,DATETIME_TYPES,NUMERIC_TYPES)

module.exports.isAllowed = isAllowed
function isAllowed(dataType) {
  if (!dataType) return false
  var type = dataType.toUpperCase()
  return _.indexOf(ALLOWED, type) == -1 ? false : true
}
