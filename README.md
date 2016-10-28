# flume-file-sink

[![Build Status](https://api.travis-ci.org/meou/flume-file-sink.svg?branch=master)](https://api.travis-ci.org/meou/flume-file-sink)

**File Sinks Implementation for Apache Flume.**

***TextFileSink***
The sink takes events from the channel, and writes events headers and bodies to a files.
Data are assuming plain text format, and the write operation of event headers is optional.

### Configuration Options

All options have default values.

#### TextFileSink
 Name                    | Default               | Description
:------------------------|:----------------------|:-----------------
filename                 | /tmp/TextFileSink.log | output file
headerIncluded           | true                  | Include header or not (true or false)
delimiter                | ":"                   | delimiter between keys and values for headers

### Configuration Example
An example flume-file.properties section for this sink :

#### TextFileSink
```
agent.sinks.textFileSink.filename = /var/sink.txt
agent.sinks.textFileSink.delimiter = ","
```

### Build your own jar
```
git clone git@github.com:meou/flume-file-sink.git
cd flume-file-sink
mvn package
cp target/flume-file-sink-{version}.jar {flume_home}/lib/flume-file-sink-{version}.jar
```

### License

This code is open source software licensed under the [Apache 2.0 License]("http://www.apache.org/licenses/LICENSE-2.0.html").
