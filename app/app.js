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
          // create the query
          const query = 'insert into sensorvalues (id, header, data) values (uuid(), :header, :data)';
          // get the body of the request
          var params = msg.payload;
          /*if(msg.payload)
            params = msg.payload;
          else
            params = msg.req.body;*/
          // parse the body if it is a string
          if(typeof params === "string")
            params = JSON.parse(params);
          // put timestamp in milliseconds
          if(params.header.timestamp)
            params.header.date = params.header.timestamp * 1000;
          // if no timestamp get current time of the server
          else
            params.header.date = new Date().getTime();

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
    RED.nodes.registerType("api-generique", saveData, {
        credentials: {
            user: {type: "text"},
            password: {type: "password"}
        }
    });
}
