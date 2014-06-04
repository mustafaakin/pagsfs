var express = require("express");
var bodyParser = require('body-parser');
var methodOverride = require('method-override');
var morgan = require('morgan');
var multer  = require('multer'); // TODO: Change it directly to busboy for streaming data, avoiding saving to disk 
var fs = require("fs");
var uuid = require('node-uuid');
var crypto = require('crypto');
var redis = require("redis");

var redisClient = redis.createClient();

var publicApp = express();
var privateApp = express();

var helper = require("./helper");
helper.setStore("/home/mustafa/pagsfs/1");


function getFileHandler(req, res) {
	var bucket = req.params.bucketName;
	var path = req.query.file;
	var timestamp = req.query.timestamp || 0;

	helper.getFile(bucket, path, timestamp, function(err, stream) {
		if (err) {
			console.log(err);
			res.send({
				status: "File not found"
			}, 404);
		} else {
			stream.pipe(res);
		}
	});
}

function getBucketHandler(req, res) {	
	var bucket = req.params.bucketName;	
	helper.getBucket(bucket, function(err, stream) {
		if (err) {
			console.log(err);
			res.send({
				status: "Bucket not found"
			}, 404);
		} else {
			stream.pipe(res);
		}
	});
}

function putFileHandler(req,res){
	var bucket = req.params.bucketName;
	var path = req.body.path;
	var file = req.files;
	var fileReadStream = fs.createReadStream(file.filedata.path);

	helper.putFile(bucket, path, fileReadStream, function(err){
		res.send("eyvallah");
	});
};

function putBucketHandler(req,res){
	var bucket = req.params.bucketName;
	var file = req.files;
	var tarStream = fs.createReadStream(file.filedata.path);

	helper.putBucket(bucket, tarStream, function(err){
		res.send("eyvallah"); // Uuu beybiii
	});
}

function getRevisionsHandler(req,res){
	var bucket = req.params.bucketName;
	var path = req.query.file;

	helper.getRevisions(bucket, path, function(err, revisions) {
		if (err) {
			res.send({
				status: err
			}, 404);
		} else {
			res.send(revisions);
		}
	});
}

function generateHMAC(){
	var message = uuid.v4();
	var secret = 'Oh my god what happened here'; // Of course this will be really secret, it is just an example..	
	hash = crypto.createHmac('sha1', secret).update(message).digest('hex');
	return hash;
}

function checkHMAC(bucket, hash, cb){
	redisClient.get([bucket], function(err, reply){
		if ( hash == reply){
			redisClient.del(bucket, function(err, reply){
				cb(true);
			});
		} else {
			cb(false);
		}
	});
}

function setHMAC(bucket, cb){
	var hash = generateHMAC();
	redisClient.set([bucket, hash], function(err, replies){
		console.log(err);
		console.log(replies);
		cb(err);
	});
}

// setHMAC("mustafa", console.log);
// checkHMAC("mustafa", "94b0139b749430ee52a3a241289f3eacc883a51c", console.log);

// Remember to Add File Size Limits
privateApp.use(multer({ dest: '/tmp/'}));
privateApp.use(morgan('dev')); // log every request to the console
privateApp.use(bodyParser()); // pull information from html in POST
privateApp.use(methodOverride()); // simulate DELETE and PUT
privateApp.get("/file/:bucketName", getFileHandler);
privateApp.get("/revisions/:bucketName", getRevisionsHandler);
privateApp.post("/file/:bucketName", putFileHandler);
privateApp.get("/bucket/:bucketName", getBucketHandler);
privateApp.post("/bucket/:bucketName", putBucketHandler);
privateApp.listen(4000);


