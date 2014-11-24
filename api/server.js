var url = require('url');
var sys = require('util');
var http = require('http');
var mysql = require('./db.js');
var config = {
    tablename: 'bet365',
    listen_port: 8080,
};

var db = null;
var stats_cache = {};

function gen_sql_analysis_cond(params, conds) {
try {
    var sqls = [];
    for (var i in conds) {
        var name = conds[i];
        if (typeof params[name + '_min'] != 'undefined')
            sqls.push(name + " >= " + db.escape(params[name + '_min']));
        if (typeof params[name + '_max'] != 'undefined')
            sqls.push(name + " <= " + db.escape(params[name + '_max']));

        // start and end comparison
        if (name.substr(-2) == '_e') {
            var subname = name.substr(0, name.length - 2);
            var comparisons = [];
            if (typeof params[name + '_lt_s'] != 'undefined'
                    && params[name + '_lt_s'])
                comparisons.push(subname + '_s < ' + name);
            if (typeof params[name + '_gt_s'] != 'undefined'
                    && params[name + '_gt_s'])
                comparisons.push(subname + '_s > ' + name);
            if (typeof params[name + '_eq_s'] != 'undefined'
                    && params[name + '_eq_s'])
                comparisons.push(subname + '_s = ' + name);
            if (comparisons.length > 0 && comparisons.length < 3)
                sqls.push('(' + comparisons.join(' OR ') + ')');
        }
    }
    if (sqls.length == 0)
        return '1';
    return sqls.join(' AND ');
} catch(e) {
    console.log(e);
    return '0';
}
}

function gen_sql_analysis_in_set(params, prefix) {
try {
    var sql = '';
    var enums = [];
    for (var key in params) {
        if (key.substr(0, prefix.length) == prefix) {
            var num = key.substr(prefix.length + 1); // prefix-value
            enums.push(db.escape(num));
        }
    }
    if (enums.length == 0)
        return '1';
    return prefix + ' IN (' + enums.join(',') + ')';
} catch(e) {
    console.log(e);
    return '0';
}
}

function gen_sql_analysis_recent(params, conds) {
try {
    var sqls = [];
    for (var i in conds) {
        var name = conds[i];
        if (typeof params[name + '_min'] != 'undefined')
            sqls.push(name + " >= " + db.escape(params[name + '_min']));
        if (typeof params[name + '_max'] != 'undefined')
            sqls.push(name + " <= " + db.escape(params[name + '_max']));
    }
    if (sqls.length == 0)
        return '1';
    return sqls.join(' AND ');
} catch(e) {
    console.log(e);
    return '0';
}
}

function route(pathname, headers, params, postdata, response) {
try {
    var tablename = config.tablename;

    uri = pathname.split('/');
    if (typeof uri[1] != "string")
        response.returnCode(404);

    switch (uri[1]) {
    case 'stats':
        if (stats_cache[uri[2]]) {
            var stat = stats_cache[uri[2]];
            console.log("Request: stats cache " + uri[2]);
            response.returnJSON(stat);
        }
        else
            response.returnCode(404);
        break;
    case 'analysis':
        if (postdata == null || postdata.length == 0)
            response.returnCode(400);
        var sql = "SELECT result, COUNT(*) as c from " + tablename + " WHERE "
            + gen_sql_analysis_cond(postdata, [
                    'eu_host_win_e',
                    'eu_draw_e',
                    'eu_guest_win_e',
                    'as_host_win_e',
                    'as_guest_win_e',
              ])
            + " AND "
            + gen_sql_analysis_in_set(postdata, 'as_dish_e')
            + " AND "
            + gen_sql_analysis_recent(postdata, [
                    'recent10_host_credit',
                    'recent10_guest_credit',
                    'recent10_host_goal',
                    'recent10_host_lose',
                    'recent10_guest_goal',
                    'recent10_guest_lose',
              ])
            + " GROUP BY result";
        console.log(sql);
        db.query(sql, function(err, data) {
            if (err) {
                response.returnCode(400, err);
                return;
            }

            var retarr = [0, 0, 0];
            for (var i in data) {
                if (data[i].result == 1)
                    retarr[0] = data[i].c;
                if (data[i].result == 0)
                    retarr[1] = data[i].c;
                if (data[i].result == -1)
                    retarr[2] = data[i].c;
            }
            response.returnJSON(retarr);
        });
        break;
    default:
        response.returnCode(404);
        break;
    }
} catch(e) {
    response.except(e);
}
}

function http_server(request, response) {
    response.requestTime = new Date();
    response.on("error", function(e) { console.log('HTTP response error: ' + e) });
    response.returnCode = function(code, msg) {
        if (msg !== null && typeof msg === "object")
            msg = msg.toString();
        if (typeof msg !== "string")
            msg = "";
        console.log("Response: HTTP " + code + " (" + msg.length + " bytes, " + (new Date() - response.requestTime) + " ms)");
        this.writeHeader(code, {'Content-Length': msg.length });
        this.write(msg);
        this.end();
    }
    response.except = function(e) {
        var message = (typeof e.message === "string") ? e.message : e.toString();

        if (typeof e.stack === "string")
            console.log(e.stack);
        else
            console.log(message);

        this.returnCode(400, message);
    }
    response.returnOK = function() {
        this.returnCode(200, "OK");
    }
    response.returnJSON = function(obj) {
        this.returnCode(200, JSON.stringify(obj));
    }
    try {
        var pathname = url.parse(request.url).pathname;
        if (pathname == "/ping") {
            response.returnCode(200, "pong");
            return;
        }
        if (request.method == "POST") {
            var data = "";
            request.on("data", function(chunk) {
                data += chunk;
            });
            request.on("end", function() {
            try {
                console.log(pathname);
                var postdata = JSON.parse(data);
                route(pathname, request.headers, request.params, postdata, response);
            } catch(e) {
                response.except(e);
            }
            });
            request.on("error", function(e) {
                response.except(e);
            });
        }
        else {
            route(pathname, request.headers, request.params, null, response);
        }
    } catch(e) {
        response.except(e);
    }
}

function loadStats() {
try {
    var tablename = config.tablename;

    var cols = [
        "eu_host_win_e",
        "eu_draw_e",
        "eu_guest_win_e",
        "as_host_win_e",
        "as_dish_e",
        "as_guest_win_e",
        "recent10_host_credit",
        "recent10_guest_credit",
        "recent10_host_goal",
        "recent10_host_lose",
        "recent10_guest_goal",
        "recent10_guest_lose"
    ];

    for (i in cols) {
        var col = cols[i];
        db.query("SELECT " + col + ", count(*) as c FROM " + tablename + " GROUP BY " + col,
            function(col) { // create a closure
                return function(err, data) {
                    if (err) {
                        console.log(err);
                        return;
                    }

                    var result = [];
                    for (var i in data) {
                        result.push([data[i][col], data[i].c]);
                    }
                    stats_cache[col] = result;
                    console.log("Loaded stats cache for " + col);
                }
            }(col));
    }
} catch(e) {
    console.log(e);
}
}

function getConnection(){
    mysql.pool.getConnection(function(err, conn){
        if (err) {
            console.log(err);
            setTimeout(getConnection, 1000);
        } else {
            new_connection(conn);
        }
    });
}

function err_handler(err) {
    console.log("MySQL connection error");
    if (!err.fatal)
        return;
    if (err.code !== 'PROTOCOL_CONNECTION_LOST')
        throw err;
    getConnection();
}

function new_connection(connection) {
    // db is global var to hold connection
    db = connection;
    console.log("MySQL connected");
    connection.on('error', err_handler);
    connection.on('end', err_handler);
    db.query('USE caipiao');
}

mysql.pool.getConnection(function(err, conn) {
    if (err)
        throw err;
    new_connection(conn);
    try {
        http.createServer(http_server).listen(config.listen_port, config.listen_host);
        console.log("Listening on " + config.listen_host + ":" + config.listen_port);
        loadStats();
    } catch(e) {
        console.log("Failed to create HTTP server on port " + config.listen_port);
        console.log(e);
    }
});
