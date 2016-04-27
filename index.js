var redis = require("redis"),
    client = redis.createClient();
var _ = require('lodash');
var Async = require('async');
var chalk = require('chalk');

// if you'd like to select database 3, instead of 0 (default), call
// client.select(3, function() { /* ... */ });

client.on("error", function (err) {
    console.log("Error " + err);
});

var size = 100000;
// Limit before the pipe is full is:
// var chunkSize = 500000;
var chunkSize = 100000;
var matchDayCount = 34;
var matchdayDayConcurrency = 34;

console.log('Usercount:     ' + size);
console.log('ChunkSize:     ' + chunkSize);
console.log('Matchdays:     ' + matchDayCount);
console.log('Concurrency:   ' + matchdayDayConcurrency);


function clearFixtures(cb) {
    var del = [];
    for (i = 0; i < 34; i++) {
        del.push('matchday:' + i);
    }
    client.del(del, cb);
}


function loadFixtures(userCount, cb) {
    var matchdays = {};
    console.log('Start building');
    for (i = 1; i <= matchDayCount; i++) {
        var key = 'matchday:' + i;
        var adds = [];
        for (l = 1; l < userCount; l++) {
            adds.push(+Math.floor((Math.random() * 60) + 1));
            adds.push('user' + l);
        }
        matchdays[key] = adds

    }
    console.log('Start Loading');
    console.time('fixtures');
    Async.forEachOfLimit(matchdays, matchdayDayConcurrency, function (matchday, key, cb) {
        var realChunkSize = chunkSize * 2;
        if (matchday.length > realChunkSize) {
            var i = 1;
            var chunks = _.chunk(matchday, realChunkSize);
            console.time('    ' + key);
            Async.each(chunks, function (chunk, next) {
                var currentCount = i;
                i++;
                console.time('    ' + key + ' #' + currentCount);
                var cmd = [key].concat(chunk);
                client.zadd(cmd, function (err, res) {
                    console.timeEnd('    ' + key + ' #' + currentCount);
                    next(err, res);
                });
            }, function (err, res) {
                console.timeEnd('    ' + key);
                cb(err);
            });
        } else {
            console.time('    ' + key);
            var cmd = [key].concat(matchday);
            client.zadd(cmd, function (err, res) {
                console.timeEnd('    ' + key);
                cb(err, res);
            });
        }
    }, function (err, res) {
        console.timeEnd('fixtures');
        cb(err);
    });
};


function union(cb) {
    var union = ['tournament', matchDayCount];
    for (i = 1; i <= matchDayCount; i++) {
        union.push('matchday:' + i);
    }
    console.time('union');
    client.ZUNIONSTORE(union, function (err) {
        console.timeEnd('union');
        cb(err);
    });
}

function check(cb) {
    var user = 'user' + Math.floor((Math.random() * size) + 1);
    var keys = _.times(matchDayCount, _.partial(_.uniqueId, 'matchday:'));
    Async.auto({
        single: function (cb) {
            Async.map(keys, function (key, next) {
                client.ZSCORE(key, user, next);
            }, cb);
        },
        total: function (cb) {
            client.ZSCORE('tournament', user, cb);
        }
    }, function (err, res) {
        var sum = _.sum(res.single);
        var total = res.total;
        if(parseInt(total) !== parseInt(sum)){
            console.log('Sum:  '+sum);
            console.log('Total:'+total);
            return cb(new Error('Sum & Total mismatch'));
        }
        cb(err);
    });
}

function read(batchCount, batchSize, readCB) {
    var chunks = [];
    for (i = 0; i < batchCount; i++) {
        var read = [];
        for (l = 0; l < batchSize; l++) {
            read.push(Math.floor((Math.random() * size) + 1))
        }
        chunks.push(read)
    }

    console.time('read');
    Async.each(chunks, function (chunk, cb) {
        var key = 'matchday:' + Math.floor((Math.random() * 34) + 1);
        Async.each(chunk, function (member, next) {
            client.zscore(key, 'user' + member, next);
        }, cb);
    }, function (err, res) {
        console.timeEnd('read');
        readCB();
    })
}

/*
 *      Actual Processing
 */

Async.series([
    clearFixtures,
    function (cb) {
        loadFixtures(size, cb);
    },
    union,
    function (cb) {
        read(10, 1000, cb);
    },
    check,
], function (err) {
    if (err) {
        throw err;
        process.exit(1);
    } else {
        console.log(chalk.green('Super done'));
        process.exit(0);
    }
});
