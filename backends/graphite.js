/*
 * Flush stats to graphite (http://graphite.wikidot.com/).
 *
 * To enable this backend, include 'graphite' in the backends
 * configuration array:
 *
 *   backends: ['graphite']
 *
 * This backend supports the following config options:
 *
 *   graphiteHost: Hostname of graphite server.
 *   graphitePort: Port to contact graphite server at.
 */

var net = require('net'),
   util = require('util');

var debug;
var flushInterval;
var graphiteHost;
var graphitePort;
var special_prefixes = false;
var altPrefixList;

var graphiteStats = {};

function matches_alt_prefix(key) {
      var use_alt_prefix = false
      util.log("matches_alt_prefix: key: " + key + " and special_prefixes: " + special_prefixes);
      if(special_prefixes) {
          util.log("matches_alt_prefix: special_prefixes: " + special_prefixes);
          altPrefixList.map( function (item) {
                                        if(key.indexOf(item) == 0)  {
                                            use_alt_prefix = true;
                                        }           
                                    });
      }
      return use_alt_prefix;
}



var post_stats = function graphite_post_stats(statString) {
  if (graphiteHost) {
    try {
      var graphite = net.createConnection(graphitePort, graphiteHost);
      graphite.addListener('error', function(connectionException){
        if (debug) {
          util.log(connectionException);
        }
      });
      graphite.on('connect', function() {
        this.write(statString);
        this.end();
        graphiteStats.last_flush = Math.round(new Date().getTime() / 1000);
      });
    } catch(e){
      if (debug) {
        util.log(e);
      }
      graphiteStats.last_exception = Math.round(new Date().getTime() / 1000);
    }
  }
}

var flush_stats = function graphite_flush(ts, metrics) {
  var statString = '';
  var numStats = 0;
  var key;

  var counters = metrics.counters;
  var gauges = metrics.gauges;
  var timers = metrics.timers;
  var averages = metrics.averages;
  var raws = metrics.raws;
  var pctThreshold = metrics.pctThreshold;

  for (key in counters) {
    var value = counters[key];
    var valuePerSecond = value / (flushInterval / 1000); // calculate "per second" rate

    var use_alt_prefix = matches_alt_prefix(key)
    if (use_alt_prefix) {
        statString += key + ' ' + valuePerSecond + ' ' + ts + "\n";
        statString += key + '_count ' + value          + ' ' + ts + "\n";
    } else {
        statString += 'stats.'        + key + ' ' + valuePerSecond + ' ' + ts + "\n";
        statString += 'stats_counts.' + key + ' ' + value          + ' ' + ts + "\n";
    }
    statString += 'stats.'        + key + ' ' + valuePerSecond + ' ' + ts + "\n";
    statString += 'stats_counts.' + key + ' ' + value          + ' ' + ts + "\n";

    numStats += 1;
  }

  for (idx in raws) {
    var use_alt_prefix = matches_alt_prefix(key)
    if (use_alt_prefix) {
        statString += raws[idx][0] + ' ' + raws[idx][1] + ' ' + raws[idx][2] + "\n";     
    } else {
        statString += 'stats.' + raws[idx][0] + ' ' + raws[idx][1] + ' ' + raws[idx][2] + "\n";
    }
    numStats += 1;
  }

  for (key in averages) {
    var vals = averages[key],
        valCount = averages[key].length,
        valTotal = 0;
    if (vals.length >= 1) {
      for (idx in vals) {
        valTotal += vals[idx];
      }
      var averageVal = valTotal / valCount;
      var use_alt_prefix = matches_alt_prefix(key)
      if (use_alt_prefix) {
          statString += key + ' ' + averageVal + ' ' + ts + "\n";
      } else {
          statString += 'stats.' + key + ' ' + averageVal + ' ' + ts + "\n";
      }

      numStats += 1;
    }
  }

  for (key in timers) {
    if (timers[key].length > 0) {
      var values = timers[key].sort(function (a,b) { return a-b; });
      var count = values.length;
      var min = values[0];
      var max = values[count - 1];

      var mean = min;
      var maxAtThreshold = max;

      var message = "";

      var key2;

      for (key2 in pctThreshold) {
        var use_alt_prefix = matches_alt_prefix(key)

        var pct = pctThreshold[key2];
        if (count > 1) {
          var thresholdIndex = Math.round(((100 - pct) / 100) * count);
          var numInThreshold = count - thresholdIndex;
          var pctValues = values.slice(0, numInThreshold);
          maxAtThreshold = pctValues[numInThreshold - 1];

          // average the remaining timings
          var sum = 0;
          for (var i = 0; i < numInThreshold; i++) {
            sum += pctValues[i];
          }

          mean = sum / numInThreshold;
        }

        var clean_pct = '' + pct;
        clean_pct.replace('.', '_');
        if (use_alt_prefix) {
            message += key + '.mean_'  + clean_pct + ' ' + mean           + ' ' + ts + "\n";
            message += key + '.upper_' + clean_pct + ' ' + maxAtThreshold + ' ' + ts + "\n";
        } else {
            message += 'stats.timers.' + key + '.mean_'  + clean_pct + ' ' + mean           + ' ' + ts + "\n";
            message += 'stats.timers.' + key + '.upper_' + clean_pct + ' ' + maxAtThreshold + ' ' + ts + "\n";
        }
      }

      if (use_alt_prefix) {
          message += key + '.upper ' + max   + ' ' + ts + "\n";
          message += key + '.lower ' + min   + ' ' + ts + "\n";
          message += key + '.count ' + count + ' ' + ts + "\n";
      } else {
          message += 'stats.timers.' + key + '.upper ' + max   + ' ' + ts + "\n";
          message += 'stats.timers.' + key + '.lower ' + min   + ' ' + ts + "\n";
          message += 'stats.timers.' + key + '.count ' + count + ' ' + ts + "\n";
      }
      statString += message;

      numStats += 1;
    }
  }

  for (key in gauges) {
    var use_alt_prefix = matches_alt_prefix(key)
    if (use_alt_prefix) {
      statString += key + ' ' + gauges[key] + ' ' + ts + "\n";
    } else {
      statString += 'stats.gauges.' + key + ' ' + gauges[key] + ' ' + ts + "\n";
    }
    numStats += 1;
  }

  statString += 'statsd.numStats ' + numStats + ' ' + ts + "\n";

  util.log(statString);
  post_stats(statString);
};

var backend_status = function graphite_status(writeCb) {
  for (stat in graphiteStats) {
    writeCb(null, 'graphite', stat, graphiteStats[stat]);
  }
};

exports.init = function graphite_init(startup_time, config, events) {
  debug = config.debug;
  graphiteHost = config.graphiteHost;
  graphitePort = config.graphitePort;

  graphiteStats.last_flush = startup_time;
  graphiteStats.last_exception = startup_time;

  if (config.hasOwnProperty('altPrefixList')) {
      util.log("config.altPrefixList is: " + config.altPrefixList + "\n\n");
      special_prefixes = true;
      altPrefixList = config.altPrefixList;
  }

  flushInterval = config.flushInterval;

  events.on('flush', flush_stats);
  events.on('status', backend_status);

  return true;
};
