//version: 1.0.0

var fs = require('fs');
var mqtt = require('mqtt');
var express = require('express');
var MongoClient = require('mongodb').MongoClient;
var morgan = require('morgan');

var Topic = '#';
var Broker_URL = 'mqtts://vps-1951290-x.dattaweb.com'; //'tcp://66.97.36.17';

var app = express();

app.use(morgan('dev'));

var caFile = fs.readFileSync("/etc/letsencrypt/live/vps-1951290-x.dattaweb.com/chain.pem");
var certFile = fs.readFileSync("/etc/letsencrypt/live/vps-1951290-x.dattaweb.com/cert.pem");
var keyFile = fs.readFileSync("/etc/letsencrypt/live/vps-1951290-x.dattaweb.com/privkey.pem");

var options = {
    rejectUnauthorized: false,
	clientId: 'MyQTT',
	port: 8883,
	username: 'points',
	password: 'Fm7G7MtV',
	keepalive: 60,
    ca: [ caFile ],
    cert: certFile,
    key: keyFile
};        

var client = mqtt.connect(Broker_URL, options);

client.on('connect', mqtt_connect);
client.on('reconnect', mqtt_reconnect);
client.on('error', mqtt_error);
client.on('message', mqtt_messageReceived);
client.on('close', mqtt_close);

function mqtt_connect(){
	client.subscribe(Topic, mqtt_suscribe);
}

function mqtt_suscribe(err, granted){

	console.log('Subscripto a ' + Topic);
	if(err){ console.log(err); }

}

function mqtt_reconnect(err){
	client = mqtt.connect(Broker_URL, options);
}

function mqtt_error(err){}

function after_publish(){}

function mqtt_messageReceived(topic, message, packet){
	insert_message(topic, message, packet);
}

function mqtt_close(){}

function extract_string(message_str){
	var message_arr = message_str.split(",");
	return message_arr;
}

var delimiter = ",";
function countInstances(message_str){
	var substrings = message_str.split(delimiter);
	return substrings.length - 1;
}

function insert_message(topic, message, packet){
	
    if(String(message) !== "hola"){
	
        const objMessage = JSON.parse(String(message));
        const objTopic = String(topic).split("/");
        
        if(typeof objMessage === 'object'){
            objMessage.topic = {
                cmd: objTopic[0],
                proveedor: objTopic[1],
                cliente: objTopic[2],
                gateway: objTopic[3]
            };
        }else{
            objMessage = {
                message: String(message)
            }
        }
        
        var url = "mongodb://localhost:27017/";
        MongoClient.connect(url, function(err, db) {
        if (err) throw err;
        const dbo = db.db("mydb");
        dbo.collection("iot_devices").insertOne(objMessage, function(err, res) {
            if (err) throw err;
            console.log("Registro insertado con exito...");
            db.close();
        });
        });

    }
}

app.get('/drop', function (req, res){
	const url = "mongodb://localhost:27017/";
	MongoClient.connect(url, function(err, db) {
	  if (err) throw err;
	  const dbo = db.db("mydb");
	  dbo.collection("iot_devices").drop(function(err, delOK) {
	    if (err) throw err;
	    res.setHeader('Content-Type', 'application/json');
        res.json({success: true, message: "Mensajes eliminados!"});  
	    db.close();
	  });
	});
})

app.get('/transmision', function(req, res) {
	const url = "mongodb://localhost:27017/";
	MongoClient.connect(url, function(err, db) {
	  if (err) throw err;
	  const dbo = db.db("mydb");
	  dbo.collection("iot_devices").find().toArray(function(err, result) {
	    if (err) throw err;
	    res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify(result));  
	    db.close();
	  });
	});
});

app.listen(8888, function(){
	console.log("Conectado al puerto 8888...");	
});