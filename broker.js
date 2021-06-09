//version: 1.0.0

var fs = require('fs');
var mqtt = require('mqtt');
var express = require('express');
var morgan = require('morgan');

var Topic = '#';
var Broker_URL = 'mqtts://vps-1951290-x.dattaweb.com'; //'tcp://66.97.36.17';

var app = express();

app.use(morgan('dev'));

app.set('PORT', 8888);

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

function mqtt_messageReceived(topic, message, packet){
	insert_message(topic, message, packet);
}

function insert_message(topic, message, packet){
    console.log(String(message));
}