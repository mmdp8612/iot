//version: 1.0.0

var fs = require('fs');
var mqtt = require('mqtt');
var express = require('express');
var MongoClient = require('mongodb').MongoClient;
var morgan = require('morgan');
var bodyParser = require('body-parser');
var cors = require('cors');

var ObjectID = require('mongodb').ObjectID;

var Topic = '#';
var Broker_URL = 'mqtts://vps-1951290-x.dattaweb.com'; //'tcp://66.97.36.17';

var app = express();

app.use(bodyParser.json());
app.use(morgan('dev'));
app.use(cors());

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

var url = "mongodb://localhost:27017/";

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

    if(String(message) === "hola") return;

    const objMessage = JSON.parse(String(message));
    const objTopic = String(topic).split("/");
    
    const idCliente = objTopic[1];

    if(typeof objMessage === 'object'){
        objMessage.topic = {
            proveedor: objTopic[0],
            cliente: objTopic[1],
            firmware: objTopic[2]
        };
    }else{
        objMessage = {
            message: String(message)
        }
    }

    MongoClient.connect(url, function(err, db) {
        if (err) throw err;
        const dbo = db.db("db_iot");
        dbo.collection("iot_messages").insertOne(objMessage, function(err, res) {
            if (err) throw err;
        });

        const query = { 
            idDevice: objMessage.idDevice, 
            topic: String(topic) 
        };
            
        const device = {
            alias: "Device Test",
            topic: String(topic),
            type: objMessage.typeDevice,
            idDevice: objMessage.idDevice,
            idCliente: idCliente
        };

        dbo.collection("iot_devices").update(query, device, {upsert: true});
        db.close();
    });
}

app.delete('/:id_device/device', function (req, res){
	MongoClient.connect(url, function (err, db) {
	    if (err) throw err;
	    const dbo = db.db("db_iot");
	    dbo.collection("iot_devices").deleteOne({_id:req.params.id_device}, function(err, result) {
            if (err) throw err;
	        res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify(result));  
	        db.close();
        });
        db.close();
	});
});

app.get('/drop', function (req, res){
	MongoClient.connect(url, function (err, db) {
	    if (err) throw err;
	    const dbo = db.db("db_iot");
	    dbo.dropCollection("iot_messages", function(err, result) {
            if (err) throw err;
        });
        dbo.dropCollection("iot_devices", function(err, result) {
            if (err) throw err;
        });
        db.close();
	});
});

app.get('/:id_cliente/devices', function(req, res) {
	MongoClient.connect(url, function(err, db) {
	  if (err) throw err;
	  const dbo = db.db("db_iot");
	  dbo.collection("iot_devices").find({idCliente:req.params.id_cliente}).sort({_id:-1}).toArray(function(err, result) {
	    if (err) throw err;
	    res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify(result));  
	    db.close();
	  });
	});
});

app.put('/:id_device/device', function(req, res) {
    MongoClient.connect(url, function(err, db) {
	    if (err) throw err;
	    const dbo = db.db("db_iot");
	    dbo.collection("iot_devices").updateOne({_id:ObjectID(req.params.id_device)}, {$set: {alias:req.body.alias}}, function(err, result) {
	        if (err) throw err;
	        res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify(result));  
	        db.close();
	    });
	});
});

app.get('/:id_cliente/:id_device/transmision', function(req, res) {
    MongoClient.connect(url, function(err, db) {
	    if (err) throw err;
	    const dbo = db.db("db_iot");
	    dbo.collection("iot_messages").find({idDevice:Number(req.params.id_device),'topic.cliente':req.params.id_cliente}).sort({_id:-1}).toArray(function(err, result) {
	        if (err) throw err;
	        res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify(result));  
	        db.close();
	    });
	});
});

/*app.get('/transmision', function(req, res) {
	MongoClient.connect(url, function(err, db) {
	  if (err) throw err;
	  const dbo = db.db("db_iot");
	  dbo.collection("iot_messages").find().toArray(function(err, result) {
	    if (err) throw err;
	    res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify(result));  
	    db.close();
	  });
	});
});*/

app.listen(app.get('PORT'), function(){
	console.log(`Connected in port ${app.get('PORT')}...`);	
});


