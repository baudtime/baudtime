module github.com/baudtime/baudtime

go 1.14

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/HdrHistogram/hdrhistogram-go v1.0.1 // indirect
	github.com/buaazp/fasthttprouter v0.1.1
	github.com/cespare/xxhash/v2 v2.1.1
	github.com/edsrzf/mmap-go v1.0.0
	github.com/fsnotify/fsnotify v1.4.7
	github.com/go-kit/kit v0.9.0
	github.com/influxdata/influxdb1-client v0.0.0-20190809212627-fc22c7df067e
	github.com/oklog/ulid v1.3.1
	github.com/opentracing/opentracing-go v1.1.0
	github.com/peterh/liner v1.1.0
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pkg/errors v0.8.1
	github.com/prometheus/client_golang v1.2.0
	github.com/prometheus/common v0.8.0
	github.com/prometheus/prometheus v2.16.0+incompatible
	github.com/satori/go.uuid v1.2.0
	github.com/shirou/gopsutil v0.0.0-20190901111213-e4ec7b275ada
	github.com/stretchr/testify v1.6.1
	github.com/tinylib/msgp v1.1.2
	github.com/uber/jaeger-client-go v2.25.0+incompatible
	github.com/uber/jaeger-lib v2.4.0+incompatible // indirect
	github.com/valyala/fasthttp v1.10.0
	go.etcd.io/etcd v0.5.0-alpha.5.0.20200824191128-ae9734ed278b
	go.uber.org/multierr v1.5.0
	golang.org/x/text v0.3.3
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	google.golang.org/grpc v1.26.0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

replace github.com/prometheus/prometheus v2.16.0+incompatible => github.com/work-chausat/prometheus v0.1.1
