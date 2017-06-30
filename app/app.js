module.exports = function(RED) {
  "use strict";
  const cassandra = require('cassandra-driver');

    function saveData(config) {
        RED.nodes.createNode(this, config);
        // cassandra
        this.host = config.host;
        this.port = config.port;
        this.keyspace = config.keyspace;
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
          var params = msg.payload;
          // parse the body if it is a string
          if(typeof params === "string")
            params = JSON.parse(params);
          
          // set or get
          /*if(params.data.action === "set")
            saveInCassandra(params, cassandraClient)
          else if(params.data.action === "get")
            getFromCassandra(params, cassandraClient)*/

          // create the query
          var query = '';
          const queryInsertWithoutId = 'insert into sensorvalues (id, header, data) values (uuid(), :header, :data)';
          const queryInsertWithId = 'insert into sensorvalues (id, header, data) values (:id, :header, :data)';

          // put timestamp in milliseconds
          if(params.header.timestamp)
            params.header.date = params.header.timestamp;
          // if no timestamp get current time of the server
          else
            params.header.date = new Date().getTime()

          // insert or update
          if(params.id) {
            query = queryInsertWithId;
          }
          else {
            query = queryInsertWithoutId;
          }

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
