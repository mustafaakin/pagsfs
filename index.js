var express = require("express");
var crypto = require('crypto');
var app = express();

function hash(pwd) {
	return  crypto.createHash('sha256').update(pwd).digest('base64');
}

app.get("/", function(req,res){
	res.send(200, {status:"alive"});
});

app.listen(4000);
