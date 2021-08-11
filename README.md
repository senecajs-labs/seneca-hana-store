![Seneca](http://senecajs.org/files/assets/seneca-logo.png)
> A [Seneca.js](https://github.com/senecajs/) a seneca-auth plugin

# seneca-hana-store

[![npm version][npm-badge]][npm-url]
[![Dependency Status][david-badge]][david-url]
[![Gitter chat][gitter-badge]][gitter-url]

| ![Voxgig](https://www.voxgig.com/res/img/vgt01r.png) | This open source module is sponsored and supported by [Voxgig](https://www.voxgig.com). |
|---|---|

seneca-hana-store is a [SAP HANA][sapcom] database plugin for the [Seneca][seneca] MVP toolkit. The plugin is using the
[hdb][nodehdb] driver.

## Install

```sh
npm install seneca-hana-store
```


## Using hana-store

    var seneca = require('seneca');
    var store = require('seneca-hana-store');

    var config = {}
    var storeopts = {
        host: '127.0.0.1',
        port: 30015,
        user: 'user',
        password: 'password',
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
    
## Test

To run tests, simply use npm:

```sh
npm test
```

## Contributing

The [Senecajs org](https://github.com/senecajs/) encourage open and safe participation. 
If you feel you can help in any way, be it with documentation, examples, 
extra testing, or new features please get in touch. 
- Before contributing please review [here](http://senecajs.org/contribute/code-of-conduct.html)  
    

##  License

Copyright Marian Radulescu and other contributors 2016, Licensed under [MIT][].

[MIT]: ./LICENSE
[npm-badge]: https://badge.fury.io/js/seneca-hana-store.svg
[npm-url]: https://badge.fury.io/js/seneca-hana-store
[david-badge]: https://david-dm.org/senecajs-labs/seneca-hana-store.svg
[david-url]: https://david-dm.org/senecajs-labs/seneca-hana-store
[gitter-badge]: https://badges.gitter.im/senecajs/seneca.png
[gitter-url]: https://gitter.im/senecajs/seneca

[sapcom]: http://discover.sap.com/hana/en-us/index.html
[seneca]: http://senecajs.org/
[nodehdb]: https://github.com/SAP/node-hdb.git

