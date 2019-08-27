# Baudtime

The goal of Baudtime is to make Prometheus more scalable, and provide extremely high write throughput.


## Features
* Designed for high throughput via TCP pipeline
* Horizontally scalable, you can send the metrics to multiple machines in one cluster and run "globally aggregated" across all data in a single place
* Highly available, data is replicated between master and its slaves and master can be failovered automatically by promoting a slave to be the new master
* Compatible with PromQL, so you can use functions over functions
* Compatible with prometheus data model(time series defined by metric name and set of key/value dimensions)
* Multi-language clients support, java and go
* Flexible to deploy: single-process, deploy gateway and datanode separately or 2in1
* Push model
  

## Building from source
    $ go get github.com/baudtime/baudtime/cmd/baudtime
    $ baudtime -log-level=info -log-dir=your_log_dir -config=your_config.toml

## Architecture, deployment
![architecture](https://raw.githubusercontent.com/baudtime/baudtime.github.io/master/baudtime.png)

## Performance
Performance comparison of a single node(https://github.com/baudtime/baudtime/tree/master/cmd/comparison) were made using an i5-7200U CPU with 8GB of RAM on Archlinux.
#### influxdb test:
```
2019/08/27 13:34:34 write data
2019/08/27 13:34:34 ---- writeClients: 3
2019/08/27 13:34:34 ---- testDataPath: /home/chausat/go/src/github.com/baudtime/baudtime/cmd/comparison/20k_tags.json
2019/08/27 13:34:34 ---- rowsPerWrite: 1000
2019/08/27 13:34:38 ---- Spent 3.034008152 seconds to insert 60000 records, speed: 19775.820299114344 Rows/Second
2019/08/27 13:34:38 read data
2019/08/27 13:34:38 ---- readClients: 2
2019/08/27 13:34:38 ---- testDataPath: /home/chausat/go/src/github.com/baudtime/baudtime/cmd/comparison/20k_tags.json
2019/08/27 13:34:58 ---- Spent 20.270831131 seconds to query 40000 records, speed: 1973.2787344288197 Rows/Second
```
#### baudtime test:
```
2019/08/27 13:38:32 write data
2019/08/27 13:38:32 ---- writeClients: 3
2019/08/27 13:38:32 ---- testDataPath: /home/chausat/go/src/github.com/baudtime/baudtime/cmd/comparison/20k_tags.json
2019/08/27 13:38:32 ---- rowsPerWrite: 1000
2019/08/27 13:38:33 ---- Spent 0.246597638 seconds to insert 60000 records, speed: 243311.33293336735 Rows/Second
2019/08/27 13:38:33 read data
2019/08/27 13:38:33 ---- readClients: 2
2019/08/27 13:38:33 ---- testDataPath: /home/chausat/go/src/github.com/baudtime/baudtime/cmd/comparison/20k_tags.json
2019/08/27 13:38:44 ---- Spent 10.441014048 seconds to query 40000 records, speed: 3831.0455111074284 Rows/Second
```


## Contributing
We are dedicate to building a high-quality time-series database. So any thoughts, pull requests, or issues are appreciated.

## Thanks
Baudtime is built on top of the awesome Prometheus(https://prometheus.io/)

## License
Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
