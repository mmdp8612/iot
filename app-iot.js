var mqtt = require('mqtt');
var mysql = require('mysql');
var express = require('express');
var Topic = 'test';
var Broker_URL = 'tcp://66.97.36.17';
var Database_URL = 'localhost';

var app = express();

var options = {
	clientId: 'MyQTT',
	port: 1883,
	username: 'points',
	password: 'Fm7G7MtV',
	keepalive: 60
};        

var connection = mysql.createConnection({
	
	host: Database_URL,
	user: 'root',
	password: '123456',
	database: 'iot'

});

connection.connect(function (err){

	if(err) throw err;

});

var client = mqtt.connect(Broker_URL, options);

client.on('connect', mqtt_connect);
client.on('reconnect', mqtt_reconnect);
client.on('error', mqtt_error);
client.on('message', mqtt_messageReceived);
client.on('close', mqtt_close);

//client.publish("test", "Hola como estas");

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

function mqtt_error(err){

	

}

function after_publish(){
	
	
	
}

function mqtt_messageReceived(topic, message, packet){

	insert_message(topic, message, packet);
	

}

function mqtt_close(){

	

}

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
	
	var MongoClient = require('mongodb').MongoClient;
	var url = "mongodb://localhost:27017/";

	MongoClient.connect(url, function(err, db) {
	  if (err) throw err;
	  var dbo = db.db("mydb");
	  var myobj = { name: "Company Inc", address: "Highway 37" };
	  dbo.collection("customers").insertOne(myobj, function(err, res) {
	    if (err) throw err;
	    console.log("1 document inserted");
	    db.close();
	  });
	});

}

app.get('/publicar', function(req, res) {
  
	client.publish(req.query.topic, req.query.message);
	
	res.send("Registro enviado...");
  
});

app.listen(8888, function(){
	
	console.log("Conectado al puerto 8000...");
	
});
