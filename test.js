var express = require('express')
var app = express()

var mysql = require('mysql');
var connection = mysql.createConnection({
    host     : '173.194.244.23',
    user     : 'root',
    password : 'e04e04',
	dateStrings: true,
});

var fs = require('fs');

app.get('/', function(req, res) {
    res.send('im the home page!');
});
app.get('/:db/', function (req, res) {
    var sql = mysql.format('use ??', req.params.db);
    console.log(sql);
    connection.query(sql, function(err, rows, fields) {
        if (err) {
            res.write('wrong database');
            res.end();
        }
    });
    connection.query('SELECT count(*), tableName, lastUpdate from updateList GROUP BY lastUpdate, tableName', function(err, rows, fields) {
        if (err) throw err;
        res.write('<table border="1">');
        for (var i in rows) {
            res.write('<tr>');
			Object.keys(rows[i]).forEach(function (key) {
            	res.write('<td>'+rows[i][key]+'</td>');
			});
            res.write('</tr>');
        }
        res.end();
    });
});
app.get('/:db/:table', function (req, res) {
    var sql = mysql.format('use ??', req.params.db);
    console.log(sql);
	console.log(req.params.table)
	Object.keys(req.query).forEach(function (key) {
		console.log(key + ': ' + req.query[key]);
	});
    connection.query(sql, function(err, rows, fields) {
        if (err) {
            res.write('wrong database');
            res.end();
        }
    });
    if (req.params.table == "alg") {
        fs.readFile(req.params.db+'.csv', function (err, data) {
            if (err) throw err;
            var ret = "<table border='1'>";
            var rows = data.toString().split("\n");
            for (var i in rows) {
                ret += "<tr><td>";
                ret += rows[i].replace(/,/g, "</td><td>");
                ret += "</td></tr>";
            }
            ret += "</table>";
            res.write(ret);
            res.end();
        }); 
        return ;
    }
    sql = mysql.format('SELECT * from ?? WHERE code = ?', [req.params.table, req.query.code]);
    console.log(sql);
    connection.query(sql, function(err, rows, fields) {
        if (err) throw err;
        res.write('<table border="1">');
		res.write('<tr>');
		for (var i in rows[0]){
			res.write('<td>'+i+'</td>');
		}
		res.write('</tr>');
        for (var i in rows) {
            res.write('<tr>');
			for (var j in rows[i]){
            	res.write('<td>'+rows[i][j]+'</td>');
			}
            res.write('</tr>');
        }
        res.end();
    });
});
var server = app.listen(1337, function () {
    var host = server.address().address
    var port = server.address().port
    console.log('Example app listening at http://%s:%s', host, port)
})



