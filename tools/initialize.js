var mysql = require('../api/db.js');
var db = null;
var config = {
    tablename: 'bet365',
};

function do_initialize() {
    var pending_count = 0;
    db.query("SELECT date, host_team, guest_team, credit_host, credit_guest, score_host, score_guest FROM bet365", function(err, data) {
        if (err) {
            console.log(err);
            return;
        }
        console.log('Got data ' + data.length + ' lines');
        for (var i in data) {
            var host_credit = 0, guest_credit = 0;
            var host_goal = 0, host_lose = 0, guest_goal = 0, guest_lose = 0;
            for (var j = i-1, count = 0; count < 10 && j>=0; j--) {
                if (data[j].host_team == data[i].host_team) {
                    host_credit += data[j].credit_host;
                    host_goal += data[j].score_host;
                    host_lose += data[j].score_guest;
                    ++count;
                }
            }
            for (var j = i-1, count = 0; count < 10 && j>=0; j--) {
                if (data[j].guest_team == data[i].guest_team) {
                    guest_credit += data[j].credit_guest;
                    guest_goal += data[j].score_guest;
                    guest_lose += data[j].score_host;
                    ++count;
                }
            }

            ++pending_count;
            if (pending_count % 1000 == 0) {
                console.log(pending_count);
            }
            db.query("UPDATE bet365 SET "
                + "recent10_host_credit=" + host_credit + ","
                + "recent10_guest_credit=" + guest_credit + ","
                + "recent10_host_goal=" + host_goal + ","
                + "recent10_host_lose=" + host_lose + ","
                + "recent10_guest_goal=" + guest_goal + ","
                + "recent10_guest_lose=" + guest_lose + " "
                + "WHERE date=? AND host_team=? AND guest_team=?",
                [data[i].date, data[i].host_team, data[i].guest_team],
                function(err, data) {
                    if (err)
                        console.log(err);
                    if (data.affectedRows != 1) {
                        console.log("The following data has affectedRows = " + data.affectedRows);
                        console.log(data[i]);
                    }
                    --pending_count;
                    if (pending_count % 10000 == 0) {
                        console.log(pending_count);
                    }
                    if (pending_count <= 0)
                        console.log('Finished!');
                });
        }
    });
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
    do_initialize();
});
