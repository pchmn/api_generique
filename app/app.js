module.exports = function(RED) {
  "use strict";
  const cassandra = require('cassandra-driver');

    function saveData(config) {
        RED.nodes.createNode(this, config);
        // cassandra
        this.host = config.host;
        this.port = config.port;
        this.keyspace = config.keyspace;
        this.table = config.table;
        var node = this;

        // cassandra user
        var authProvider = null;
        if (node.credentials.user) {
            authProvider = new cassandra.auth.PlainTextAuthProvider(
                node.credentials.user,
                node.credentials.password
            );
        }
        // connect to cassandra
        var cassandraClient = new cassandra.Client({
            contactPoints: node.host.replace(/ /g, "").split(","),
            keyspace: node.keyspace,
            authProvider: authProvider
        });

        node.on('input', function(msg) {
          // get the body of the request
          var sensorValues = msg.payload.cassandraSensorValues;
          var genericFormat = msg.payload.cassandraGenericFormat;
          // get table to save in
          var table = msg.table || node.table;
          
          // set or get
          /*if(params.data.action === "set")
            saveInCassandra(params, cassandraClient)
          else if(params.data.action === "get")
            getFromCassandra(params, cassandraClient)*/

          // create the query
          var querySensorValues = '';
          var queryGenericFormat = '';
          const queryInsertWithoutId = function(table) {
            return 'insert into '+ table +' (id, header, data) values (uuid(), :header, :data)';
          };
          const queryInsertWithId = function(table) {
            return 'insert into '+ table +' (id, header, data) values (:id, :header, :data)';
          };

          // put timestamp in milliseconds
          if(sensorValues.header.timestamp) {
            sensorValues.header.date = sensorValues.header.timestamp;
            genericFormat.header.date = sensorValues.header.timestamp;
          }
          // if no timestamp get current time of the server
          else {
            sensorValues.header.date = new Date().getTime() / 1000;
            genericFormat.header.date = new Date().getTime() / 1000;
          }

          // insert or update
          if(msg.payload.id) {
            querySensorValues = queryInsertWithId(table);
            queryGenericFormat = queryInsertWithId('genericformat_' + table);
          }
          else {
            querySensorValues = queryInsertWithoutId(table);
            queryGenericFormat = queryInsertWithoutId('genericformat_' + table);
          }

          // execute the query
          cassandraClient.execute(querySensorValues, sensorValues, { prepare: true })
            .then(cassandraClient.execute(queryGenericFormat, genericFormat, { prepare: true }))
            .then(result => {
                msg.payload = {};
                msg.payload.result = "success";
                msg.payload.objectSaved = sensorValues;
                node.send(msg);
            })
            .catch(err => {
              node.error(err, msg);
            })
        });
    }

    function saveInCassandra(params, cassandraClient) {
      // create the query
      const query = 'insert into sensorvalues (id, header, data) values (uuid(), :header, :data)';;
      const queryInsert = 'insert into sensorvalues (id, header, data) values (uuid(), :header, :data)';
      const queryUpdate = 'insert into sensorvalues (id, header, data) values (:id, :header, :data)'

      // put timestamp in milliseconds
      if(params.header.timestamp)
        params.header.date = params.header.timestamp * 1000;
      // if no timestamp get current time of the server
      else
        params.header.date = new Date().getTime()

      // insert or update
      /*if(params.header.id) {
        params.id = params.header.id
        query = queryUpdate;
      }
      else {
        query = queryInsert;
      }*/

      // execute the query
      cassandraClient.execute(query, params, { prepare: true })
        .then(result => {
            msg.payload = {};
            msg.payload.result = "success";
            msg.payload.objectSaved = params;
            node.send(msg);
        })
        .catch(err => {
          node.error(err, msg);
        })
    }

    function getFromCassandra(params, cassandraClient) {

    }

    RED.nodes.registerType("api-generique", saveData, {
        credentials: {
            user: {type: "text"},
            password: {type: "password"}
        }
    });
}
