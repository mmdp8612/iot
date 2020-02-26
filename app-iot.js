var mqtt = require('mqtt');
var mysql = require('mysql');
var express = require('express');
var Topic = 'test';
var Broker_URL = 'tcp://66.97.40.26';
var Database_URL = 'localhost';

var app = express();

var options = {
	clientId: 'MyQTT',
	port: 1883,
	username: 'silics',
	password: 'Barty88',
	keepalive: 60
};        

var connection = mysql.createConnection({
	
	host: Database_URL,
	user: 'root',
	password: 'password',
	database: 'iot',
	port: 3306,
	insecureAuth : true

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
	
	var t = topic.split("/");
	
	var id_usuario = 15;
	
	
	
	var out = message.toString().replace(/-/gi, ">>").split("|");
	
	console.log(out);

	var id = [];

	var id_dispositivo = 0;

	var mensaje = '';

	var version = null;

	var tipo = null;

	var firmware = null;

	out.forEach((o, i) => {
		
		id = o.split(":");
	  
		if(id[0] == 'ID'){
			
			id_dispositivo = id[1];
			
		}else if(id[0] == 'M'){
			
			mensaje = id[1].toString();	
			
		}else if(id[0] == 'V'){
			
			version = id[1];
			
			var v = version.split(".");
			
			tipo = v[0];
			
			firmware = v[1] + "." + v[2];
			
		}
	  
	});
	
	var clientID = 'Canal01';
	
	var dispositivo = '001B44113AB7';
	
	var params = ['mensajes_iot', 'idUsuario', 'idDispositivo', 'tipoDispositivo', 'topic', 'mensaje', 'firmware', id_usuario, id_dispositivo,tipo,topic,mensaje,firmware];
	
	var sql = 'INSERT INTO ?? (??,??,??,??,??,??) VALUES (?,?,?,?,?,?)';
	
	sql = mysql.format(sql, params);
	
	connection.query(sql, function (err, results){
		
		//connection.release();
		
		if(err) throw err;
		
		console.log("1 registro insertado");

	});

}

app.get('/publicar', function(req, res) {
  
	client.publish(req.query.topic, req.query.message);
	
	res.send("Registro enviado...");
  
});

app.listen(8000, function(){
	
	console.log("Conectado al puerto 8000...");
	
});
