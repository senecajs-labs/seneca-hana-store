seneca-hana-store
=================

seneca-hana-store is a [SAP HANA][sapcom] database plugin for the [Seneca][seneca] MVP toolkit. The plugin is using the
[node-hana-odbc][hanaodbc] driver.

Usage:

    var seneca = require('seneca');
    var store = require('seneca-hana-store');

    var config = {}
    var storeopts = {
      dsn:'HANA',
      username:'user',
      password:'password'
      schema: 'schema'
    };

    ...

    var si = seneca(config)
    si.use(store, storeopts)
    si.ready(function() {
      var product = si.make('product')
      ...
    })
    ...

[sapcom]: http://www54.sap.com/pc/tech/in-memory-computing-hana/software/overview/index.html
[seneca]: http://senecajs.org/
[hanaodbc]: https://github.com/rksm/node-hana-odbc.git
