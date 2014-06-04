var express = require("express");
var bodyParser = require('body-parser');
var methodOverride = require('method-override');
var morgan = require('morgan');
var multer = require('multer'); // TODO: Change it directly to busboy for streaming data, avoiding saving to disk 
var fs = require("fs");
var uuid = require('node-uuid');
var crypto = require('crypto');
var redis = require("redis");
var p = require("path");

var redisClient = redis.createClient();

var publicApp = express();
var privateApp = express();



var helper = require("./helper");
var store = "/home/mustafa/pagsfs/1";
helper.setStore(store);


function getFileHandler(req, res) {
	var bucket = req.params.bucketName;
	var path = req.query.path;
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

function putFileHandler(req, res) {
	var bucket = req.params.bucketName;
	var path = req.body.path;
	var file = req.files;
	var fileReadStream = fs.createReadStream(file.filedata.path);

	helper.putFile(bucket, path, fileReadStream, function(err) {
		res.send("eyvallah");
	});
};

function putBucketHandler(req, res) {
	var bucket = req.params.bucketName;
	var file = req.files;
	var tarStream = fs.createReadStream(file.filedata.path);

	helper.putBucket(bucket, tarStream, function(err) {
		res.send("eyvallah"); // Uuu beybiii
	});
}

function getRevisionsHandler(req, res) {
	var bucket = req.params.bucketName;
	var path = req.query.path;

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

function generateHMAC() {
	var message = uuid.v4();
	var secret = 'Oh my god what happened here'; // Of course this will be really secret, it is just an example..	
	hash = crypto.createHmac('sha1', secret).update(message).digest('hex');
	return hash;
}

function checkHMAC(bucket, hash, cb) {
	redisClient.get([bucket], function(err, reply) {
		if (reply != null && hash == reply) {
			redisClient.del(bucket, function(err, reply) {
				cb(true);
			});
		} else {
			cb(false);
		}
	});
}

function setHMAC(bucket, cb) {
	var hash = generateHMAC();
	redisClient.set([bucket, hash], function(err, replies) {
		cb(err, hash);
	});
}

function paramChecker(req, res, next) {
	// Prevent path traversal errors
	// See if bucket is valid
	var bucket = req.params.bucketName;
	if ( bucket == null || typeof bucket == undefined){
		res.send({
			status: "Bucket name is not right:" + bucket
		}, 401);
	} else {
		// TODO: Add try catch to here 
		var relation = p.relative(store, p.join(store, bucket));
		var isBucketWrong = relation.length >= 2 && relation.substr(0, 2) == "..";
		if (isBucketWrong) {
			res.send({
				status: "Bucket name is not right:" + bucket
			}, 401);
		} else {
			// There does not have to be a path all the time
			var path = req.query.path;
			if (path) {
				var relation = p.relative(store, p.join(store, path));
				var isPathWrong = relation.length >= 2 && relation.substr(0, 2) == "..";
				if (isBucketWrong) {
					res.send({
						status: "Path name is not right:" + path
					}, 401);
				} else {
					next();
				}
			} else {
				next();
			}
		}
	}
}

function hmacChecker(req, res, next) {
	var bucket = req.params.bucketName;
	var code = req.query.code;
	if (code == null || typeof code == undefined) {
		res.send({
			status: "Code value does not match"
		}, 401);
	} else {
		checkHMAC(bucket, code, function(isValid) {
			console.log(bucket, code);
			console.log(isValid);
			if (isValid) {
				next();
			} else {
				res.send({
					status: "Code value does not match"
				}, 401);
			}
		});
	}
}

function setPublicAccess(req, res) {
	var bucketName = req.params.bucketName;
	setHMAC(bucketName, function(err, hash) {
		if (!err) {
			res.send({
				code: hash
			});
		} else {
			res.send(500); // which 5xx?
		}
	});
}

// Remember to Add File Size Limits
privateApp.use(multer({
	dest: '/tmp/'
}));
privateApp.use(morgan('dev')); // log every request to the console
privateApp.use(bodyParser()); // pull information from html in POST
privateApp.use(methodOverride()); // simulate DELETE and PUT


privateApp.get("/file/:bucketName", paramChecker, getFileHandler);
privateApp.get("/revisions/:bucketName", paramChecker, getRevisionsHandler);
privateApp.post("/file/:bucketName", paramChecker, putFileHandler);
privateApp.get("/bucket/:bucketName", paramChecker, getBucketHandler);
privateApp.post("/bucket/:bucketName", paramChecker, putBucketHandler);
privateApp.get("/setPublicAccess/:bucketName",paramChecker, setPublicAccess);

// Eliminate this code copy paste by Using Express 4.0 Router
publicApp.use(multer({
	dest: '/tmp/'
}));
publicApp.use(morgan('dev')); // log every request to the console
publicApp.use(bodyParser()); // pull information from html in POST
publicApp.use(methodOverride()); // simulate DELETE and PUT


publicApp.get("/file/:bucketName",  paramChecker, hmacChecker, getFileHandler);
publicApp.get("/revisions/:bucketName",  paramChecker, hmacChecker, getRevisionsHandler);
publicApp.post("/file/:bucketName",  paramChecker, hmacChecker, putFileHandler);
publicApp.get("/bucket/:bucketName", paramChecker, hmacChecker, getBucketHandler);
publicApp.post("/bucket/:bucketName",  paramChecker, hmacChecker, putBucketHandler);

privateApp.listen(4000);
publicApp.listen(5000);