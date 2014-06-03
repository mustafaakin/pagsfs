var archiver = require("archiver");
var express = require("express");
var redis = require("redis");
var uuid = require('node-uuid');
var crypto = require('crypto');
var tar = require('tar-stream');
var async = require("async");

var client = require("redis").createClient();
var lock = require("redis-lock")(client);

var fs = require("fs");
var p = require("path");
var mkdirp = require("mkdirp");

var color = require("colors");
// Network Isolation upto user

// Safe Listen Port, no auth needed, adding new nodes
// Unsafe Listen Port, one time key based gets and puts

// require 
/* 
	Methods: 
		- create: 
		- getFile: 
		- putFile: Create if does not exist, else version it if specified, get lock for folder!
		- getFolder: Check if data exits, tar.gz them
		- putFolder: Create if does not exist, else version all folder contents if not specified
		- copyFolder: Create a new exact copy of folder from source to target
		- deleteFolder: Delete folder, not meant to be used
		- status: 
		- health:
*/


/*
#### Bucket storage structure:
	meta.json
	archive.tar
	OBJECT0000001
	OBJECT0000002
	OBJECT0000003
	OBJECT0000004
	OBJECT0000005
	OBJECT0000006
	...

	meta.json = {
		name: ...
		replicate: boolean
		files: {
			"abc/def.ad": {
				timestamp1: {
					shasum: 123123123,
					name: OBJECTNAME1
				}
			}
		}
	}
	on file update, determine the changes and replicate the metadata and the objects to others
*/

var store = "/home/mustafa/pagsfs/1";

function createBucket(bucket, options, cb) {
	// Check if options replicate is true or false, nothing else	
	var emptyMetaData = {
		name: bucket,
		replicate: options.replicate || false,
		files: {}
	};

	var exactFilePath = p.join(store, bucket);
	fs.stat(exactFilePath, function(err, stat) {
		if (err) {
			mkdirp(exactFilePath, function(err, stat) {
				var metaJSONString = JSON.stringify(emptyMetaData);
				var metaJSONPath = p.join(exactFilePath, "meta.json");
				fs.writeFile(metaJSONPath, metaJSONString, function(err) {
					cb(err, true);
				});
			});
		} else {
			console.error("Bucket", bucket, "already exists");
			cb(false);
		}
	});
}

/*
putFile("mustafa", "bigfile", fs.createReadStream("bigfile"), function(){
	client.end();
});
*/

/*
getFile("mustafa", "file1", 0, function(err, stream) {
	stream.on("data", function(data) {
		console.log(data.toString());
	});
	console.log("Error", err);
});
*/

function readMeta(bucket, cb) {
	var metaPath = p.join(store, bucket, "meta.json");
	fs.readFile(metaPath, function(err, data) {
		if (err) {
			cb(false);
		} else {
			var meta = JSON.parse(data);
			cb(meta);
		}
	});
}

function saveMeta(bucket, meta, cb) {
	var metaPath = p.join(store, bucket, "meta.json");
	var metaJSON = JSON.stringify(meta);

	fs.writeFile(metaPath, metaJSON, function(err) {
		cb(err);
	});
}

function getFile(bucket, path, timestamp, cb) {
	// Filter path for path traversal attacks
	// if 0 return latest
	var metaPath = p.join(store, bucket, "meta.json");
	fs.readFile(metaPath, function(err, data) {
		if (err) {
			cb("Meta data cannot be read");
		} else {
			var meta = JSON.parse(data);
			var files = meta.files;
			var file = meta.files[path];
			if (file) {
				var version = null;
				if (timestamp == 0) {
					var newest = 0;
					for (var time in file) {
						if (time > newest) {
							newest = time;
						}
					}
					version = file[newest];
				} else {
					version = file[timestamp];
				}
				if (version) {
					var streamPath = p.join(store, bucket, version.name);
					var stream = fs.createReadStream(streamPath);
					// Check if file exists here!
					cb(null, stream);
				} else {
					cb("File with given timestamp not found");
				}
			} else {
				cb("File not found");
			}
		}
	});
	// 1. Check if folder exists from disk
	// 2. If not exists, ask all peers by broadcasting (for one time checks, check key first to reduce network)
	// 3. If no one has data, return 404
	// 4. If data exists, stream from disk or network 
}

function putFile(bucket, path, fileReadStream, cb) {
	lock("pagsfs-" + bucket, 10000, function(done) {
		console.log("Lock acquired for ", bucket.red, " for file ", path.green);
		readMeta(bucket, function(meta) {
			if (!meta) {
				console.log("no meta :/")
				// Handle error
			} else {
				// EMPTY FILE STREAM ERRORS!!!
				var files = meta.files;
				var file = meta.files[path];

				if (!file) {
					meta.files[path] = {}
				}
				// Check for replication status
				// Delete old copies if needed
				var objectName = uuid.v4();
				console.log("new object for bucket", bucket.red, objectName.blue);
				var objectPath = p.join(store, bucket, objectName);
				var fileWriteStream = fs.createWriteStream(objectPath);

				var hash = crypto.createHash('sha1');
				hash.setEncoding('hex');

				fileReadStream.on("error", function() {
					done();
					console.log("error file stream :/");
				});

				fileReadStream.on('end', function() {
					hash.end();
					var computedHash = hash.read();
					var time = (new Date()).getTime();
					meta.files[path][time] = {
						hash: computedHash,
						name: objectName,
					}

					saveMeta(bucket, meta, function(err) {
						cb(err);
						done();
					});
					// Sync!!
				});

				fileReadStream.pipe(hash);
				fileReadStream.pipe(fileWriteStream);
			}
		});
	});
	// 0. Lock folder
	// 1. Check if folder & file exists
	// 2. If file exists, version the old one and notify peers
	// 3. Stream the file to disk and other peers
	// Possible enhancement: Remove the old tar.gz
}

function oldestObjectInFile(file) {
	var newest = 0;
	for (var time in file) {
		if (time > newest) {
			newest = time;
		}
	}
	return file[newest];
}

/*
createBucket("mustafa", {revision:true}, function(){
	putFile("mustafa", "file1", fs.createReadStream("file1"), function(){});
	putFile("mustafa", "file2", fs.createReadStream("file2"), function(){});
});
*/
// putFile("mustafa", "file1", fs.createReadStream("file1_new"), function(){});


/*
getBucket("mustafa", function(stream) {
	if (stream) {
		console.log("uu beybi");
		var writeStream = fs.createWriteStream("test.tar");
		stream.pipe(writeStream);
	} else {
		console.log("yaa :/");
	}
});
*/


function getBucket(bucket, cb) {
	readMeta(bucket, function(meta) {
		if (!meta) {
			cb(null);
		} else {
			var files = meta.files;
			var archive = archiver("tar");
			for (var filename in files) {
				var latest = oldestObjectInFile(files[filename]);
				if (!latest) {
					// empty file/non existent file
				} else {
					var objectPath = p.join(store, bucket, latest.name);
					archive.append(fs.createReadStream(objectPath), {
						name: filename
					});
				}
			}
			archive.finalize();
			cb(archive);
		}
	});
	// 1. Check if folder exists from disk
	// 2. If folder does not exist, ask peers
	// 3. IF no one has data, return 404
	// 4. If data exists, stream tar
	// Possible enhancement: Determimne file date & cache .tar.gz
}

function putBucket(bucket, tarStream, putBucketCallback) {
	// Load all meta
	// Get the file list
	// Prepare a new meta by using old meta
	// Delete other contents
	lock("pagsfs-" + bucket, 10000, function(done) {
		readMeta(bucket, function(meta) {
			if (!meta) {
				putBucketCallback("no meta man wtf");
			} else {
				// Unlink old files
				var toUnlink = [];
				for (var filename in meta.files) {
					var file = meta.files[filename];
					console.log(file);
					for (var timestamp in file) {
						toUnlink.push(file[timestamp].name);
					}
				}

				// Unlink files
				async.each(toUnlink, function(file, callback) {
					console.log(store, bucket, file);
					var filePath = p.join(store, bucket, file);
					fs.unlink(filePath, callback);
				}, function(err) {
					console.log(err);

					meta.files = {};

					var extract = tar.extract();

					extract.on('entry', function(header, entryStream, entryCallback) {
						var hash = crypto.createHash('sha1');
						hash.setEncoding('hex');

						var objectName = uuid.v4();
						var filePath = header.name;
						var destinationPath = p.join(store, bucket, objectName);
						var destinationStream = fs.createWriteStream(destinationPath);

						entryStream.on("error", function() {
							putBucketCallback("file read error");
							console.log("error file stream :/");
						});

						entryStream.on('end', function() {
							hash.end();
							var computedHash = hash.read();
							var time = (new Date()).getTime();

							meta.files[filePath] = {};
							meta.files[filePath][time] = {
								hash: computedHash,
								name: objectName,
							}
							entryCallback();
						});

						entryStream.pipe(hash);
						entryStream.pipe(destinationStream);
					});
					tarStream.pipe(extract);

					extract.on('finish', function(){
						console.log("extract finish");
						saveMeta(bucket, meta, function(err) {
							putBucketCallback(err);
							done();
						});
					});
				});
			}
		});
	});
}

var tarReadStream = fs.createReadStream("aq.tar");
putBucket("myaq", tarReadStream, console.log);