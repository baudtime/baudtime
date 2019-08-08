# Baudtime
Baudtime lets Prometheus horizontally scalable, and provide you an extremely high write throughput.


## Features
* Designed for high throughput via tcp pipeline
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

## Contributing
We are dedicate to building high-quality time series storage. So any thoughts, pull requests, or issues are appreciated.

## Thanks
Thanks prometheus, it is a very awesome application, baudtime is built on the top of it.

## License
Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).