'use strict'

const fs = require('fs')
    , restify = require('restify')
    , log = require('npmlog-ts')
    , async = require('async')
    , commandLineArgs = require('command-line-args')
    , getUsage = require('command-line-usage')
    , _ = require('lodash')
;

log.timestamp = true;

// Initialize input arguments
const optionDefinitions = [
  { name: 'help', alias: 'h', type: Boolean },
  { name: 'racefile', alias: 'r', type: String },
  { name: 'raceid', alias: 'i', type: Number },
  { name: 'demozone', alias: 'd', type: String },
  { name: 'verbose', alias: 'v', type: Boolean, defaultOption: false }
];

const sections = [
  {
    header: 'IoT Racing Race Simulator',
    content: 'Race simulator for IoTCS testing & stressing'
  },
  {
    header: 'Options',
    optionList: [
      {
        name: 'racefile',
        typeLabel: '[underline]{file}',
        alias: 'r',
        type: String,
        description: 'Race data used for simulation'
      },
      {
        name: 'raceid',
        typeLabel: '[underline]{number}',
        alias: 'i',
        type: Number,
        description: 'Race ID used for the simulation'
      },
      {
        name: 'demozone',
        typeLabel: '[underline]{MADRID|BARCELONA|LISBON|PARIS|AMSTERDAM|MILAN|BERLIN|MUNICH}',
        alias: 'd',
        type: String,
        description: 'Demozone used for the simulation'
      },
      {
        name: 'verbose',
        alias: 'v',
        description: 'Enable verbose logging.'
      },
      {
        name: 'help',
        alias: 'h',
        description: 'Print this usage guide.'
      }
    ]
  }
]
const validDemozones = ['MADRID','BARCELONA','LISBON','PARIS','AMSTERDAM','MILAN','BERLIN','MUNICH'];
var options = undefined;

try {
  options = commandLineArgs(optionDefinitions);
} catch (e) {
  console.log(getUsage(sections));
  console.log(e.message);
  process.exit(-1);
}

if (options.help) {
  console.log(getUsage(sections));
  process.exit(0);
}

if (!options.racefile || !options.raceid || !options.demozone) {
  console.log(getUsage(sections));
  process.exit(-1);
}

const raceDataFile = options.racefile;
const raceId       = options.raceid;
const demozone     = options.demozone;
const dummyDate    = "01/01/2000 ";
const iotcswrapper = "http://localhost:8888"
const DATAURI      = "/iot/send/data/";
const ALERTURI     = "/iot/send/alert/";

if (!_.includes(validDemozones, demozone)) {
  log.error("", "Invalid demozone %s", demozone);
  process.exit(-1);
}

log.level = (options.verbose) ? 'verbose' : 'info';

if (!fs.existsSync(raceDataFile)) {
  log.error("", "File %s does not exist or is not readable", raceDataFile);
  process.exit(-1);
}

var contents = fs.readFileSync(raceDataFile,'utf8');
var lines = contents.split("\n");

log.info("", "Processing race file: %s (%d entries)", raceDataFile, lines.length);
log.info("", "Simulating race id %d for demozone %s", raceId, demozone);

var client = restify.createJsonClient({
  url: iotcswrapper,
  connectTimeout: 1000,
  requestTimeout: 1000,
  retry: false,
  headers: {
    "content-type": "application/json"
  }
});

var counter = [];

async.eachOfSeries(lines, (line, index, callback) => {
  var currentLine;
  var t2 = undefined;
  var data;

  async.series([
    function(c) {
      currentLine = line;
      data = currentLine.split(";");
      if ( index > 0) {
        var nextLine = lines[index - 1];
        var nextData = nextLine.split(";");
        t2 = nextData[0];
      }
      c(null);
    },
    function(c) {
      var t1 = data[0];
      if (t2) {
        var d1 = new Date(dummyDate + t1);
        var d2 = new Date(dummyDate + t2);
        var wait = d1 - d2;
        setTimeout(() => {
          c(null);
        }, wait);
      } else {
        c(null);
      }
    },
    function(c) {
      var eventType = data[1];
      var urn = data[2];
      var payload = data[3];
      var payloadJSON = JSON.parse(payload);

      var e = _.find(counter, { 'urn': urn });
      if ( !e) {
        counter.push( { urn: urn, messages: 0 } );
        e = _.find(counter, { 'urn': urn });
      }

      payloadJSON.raceId = raceId;
      payloadJSON.demozone = demozone;
      log.verbose("", "[%d] - Sending [%s] to '%s' with %s", index, eventType, urn, JSON.stringify(payloadJSON));
      if (!options.verbose) {
        process.stdout.write(".");
      }
      var uri = (eventType === 'data') ? '/iot/send/data/' : '/iot/send/alert/';
      uri += urn;
      client.post(uri, payloadJSON, function(err, req, res, obj) {
        if (err) {
          log.error("",err.message);
          process.exit(-1);
        }
        log.verbose("",'%d -> %j', res.statusCode, res.headers);
        log.verbose("",'%j', obj);
        e.messages++;
      });
      callback(null);
      c(null);
    }
  ]);
}, function(err) {
  if (!options.verbose) {
    process.stdout.write("\n");
  }
  log.info("", "Processing completed");
  _.each(counter, (c) => {
    log.info("", c.urn + ": " + c.messages);
  });
});
