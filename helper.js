var archiver = require("archiver");

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


var store = "."; // default store


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

function putFile(bucket, path, fileReadStream, putFileCallback) {
	lock("pagsfs-" + bucket, 10000, function(done) {
		readMeta(bucket, function(meta) {
			if (!meta) {
				putFileCallback("Metadata not found for: " + bucket);
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

				var objectPath = p.join(store, bucket, objectName);
				var fileWriteStream = fs.createWriteStream(objectPath);

				var hash = crypto.createHash('sha1');
				hash.setEncoding('hex');

				fileReadStream.on("error", function() {
					done();
					putFileCallback("Cannot read from stream" + bucket + ", " + path);
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
						putFileCallback(err);
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

function getBucket(bucket, getBucketCallback) {
	readMeta(bucket, function(meta) {
		if (!meta) {
			getBucketCallback("Bucket not found");
		} else {
			var files = meta.files;
			var archive = archiver("tar");
			for (var filename in files) {
				var latest = oldestObjectInFile(files[filename]);
				if (!latest) {
					getBucketCallback("Internal error, file not found");
					// empty file/non existent file
				} else {
					var objectPath = p.join(store, bucket, latest.name);
					archive.append(fs.createReadStream(objectPath), {
						name: filename
					});
				}
			}
			archive.finalize();
			getBucketCallback(null, archive);
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
				putBucketCallback("Meta not found");
			} else {
				// Unlink old files
				var toUnlink = [];
				for (var filename in meta.files) {
					var file = meta.files[filename];
					for (var timestamp in file) {
						toUnlink.push(file[timestamp].name);
					}
				}

				// Unlink files
				async.each(toUnlink, function(file, callback) {
					var filePath = p.join(store, bucket, file);
					fs.unlink(filePath, callback);
				}, function(err) {

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
							putBucketCallback("File cannot be read..");
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



module.exports.setStore = function(givenStore){
	store = givenStore;
}

module.exports.getFile = getFile;
module.exports.putFile = putFile;
module.exports.getBucket = getBucket;
module.exports.putBucket = putBucket;