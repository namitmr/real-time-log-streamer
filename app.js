var express = require("express");
var path = require('path');
var fs = require('fs');
var request = require('request');
var querystring = require('querystring');
var url = require('url');
var conf = require('config');
var cookieParser = require('cookie-parser');
var bodyParser = require('body-parser');
var session = require('express-session');
var url = require('url');
var sprintf = require("sprintf-js").sprintf;
var httpProxy = require("http-proxy");
var http = require('http');
var fs = require('fs');
var filename = 'target.json';
var mysql = require('mysql');
var Pool = require('generic-pool').Pool;
var expressValidator = require('express-validator');
var Sequelize = require('sequelize');
var MongoClient = require('mongodb').MongoClient;
var format = require('util').format;
const fileUpload = require('express-fileupload');
var Kafka = require('node-rdkafka');
var Tail = require('tail').Tail;



global.appRoot = path.resolve(__dirname);



var PORT = conf.get('port');
var logsPath = conf.get('logsPath');
global.kafkaUrl = conf.get('kafkaUrl');
global.topic = conf.get('topic-name');
var app = express();

var producer = new Kafka.Producer({
  'client.id': 'kafka',
  'metadata.broker.list': global.kafkaUrl,
  'compression.codec': 'gzip',
  'retry.backoff.ms': 200,
  'message.send.max.retries': 10,
  'socket.keepalive.enable': true,
  'queue.buffering.max.messages': 100000,
  'queue.buffering.max.ms': 1000,
  'batch.num.messages': 1000000,
  'dr_cb': true
});




console.log(sprintf("using env: [%s]", app.get('env')));
app.set('view engine', 'html');
app.set('view engine', 'pug');
app.use(express.static(path.join(__dirname, 'public')));
app.use(cookieParser());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(fileUpload());
app.use(expressValidator());


tail = new Tail(logsPath);
producer.connect();
tail.on("line", function(data) {
  console.log(data);
  var coeff = 1000 * 60 * 1;
  var array = data.split(" - ");
  var logTimestamp = new Date(array[1]);
  var roundedTimestamp = new Date(Math.round(logTimestamp.getTime() / coeff) * coeff);
  var obj = {
  	"IP" : array[0],
  	"loglevel" : array[2],
  	"time" : roundedTimestamp.toISOString(),
  	"requestType" : array[3]
  };
  producer.on('ready', function() {
	  try {
	    var tmp = producer.produce(
	      // Topic to send the message to
	      global.topic,
	      // optionally we can manually specify a partition for the message
	      // this defaults to -1 - which will use librdkafka's default partitioner (consistent random for keyed messages, random for unkeyed messages)
	      null,
	      // Message to send. Must be a buffer
	      new Buffer(JSON.stringify(obj)),
	      // for keyed messages, we also specify the key - note that this field is optional
	      null,
	      // you can send a timestamp here. If your broker version supports it,
	      // it will get added. Otherwise, we default to 0
	      Date.now()
	      // you can send an opaque token here, which gets passed along
	      // to your delivery reports
	    );
	    console.log("RES : ",tmp);
	  } catch (err) {
	    console.error('A problem occurred when sending our message');
	    console.error(err);
	  }
	});
  producer.on('event.error', function(err) {
  console.error('Error from producer');
  console.error(err);
})
});

tail.on("error", function(error) {
  console.log('ERROR: ', error);
});








app.listen(PORT) ;
console.log(sprintf('Listening on port %s...', PORT));
