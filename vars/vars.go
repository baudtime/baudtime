package vars

import (
	"flag"
	"io"
	"net"
	"os"

	osutil "github.com/baudtime/baudtime/util/os"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	AppName        = "baudtime"
	CpuProfile     string
	MemProfile     string
	ConfigFilePath string
	Logger         log.Logger
	LogWriter      io.Writer
	LocalIP        string
	LocalAddr      string
	PageSize       = os.Getpagesize()
	PromRegistry   = prometheus.NewRegistry()
)

func Init() {
	logFile := flag.String("log.file", "", "logs will be written to this file")
	logLevel := flag.String("log.level", "info", "log level")
	flag.StringVar(&ConfigFilePath, "config.file", AppName+".toml", "configure file path")
	flag.StringVar(&CpuProfile, "profile.cpu", "", "write cpu profile to file")
	flag.StringVar(&MemProfile, "profile.mem", "", "write memory profile to file")
	flag.Parse()

	var err error

	if debugIP, found := os.LookupEnv("debugIP"); found {
		LocalIP = debugIP
	} else {
		LocalIP, err = osutil.GetLocalIP()
		if err != nil {
			panic(err)
		}
	}

	if ip := net.ParseIP(LocalIP); ip == nil {
		panic("invalid ip address")
	}

	if *logFile == "" {
		LogWriter = os.Stdout
	} else {
		LogWriter = &lumberjack.Logger{
			Filename:   *logFile,
			MaxSize:    256, // megabytes
			MaxBackups: 3,
			MaxAge:     28, //days
		}
	}

	var levelOpt level.Option
	switch *logLevel {
	case "error":
		levelOpt = level.AllowError()
	case "warn":
		levelOpt = level.AllowWarn()
	case "info":
		levelOpt = level.AllowInfo()
	case "debug":
		levelOpt = level.AllowDebug()
		if LogWriter != os.Stdout {
			LogWriter = io.MultiWriter(LogWriter, os.Stdout)
		}
	default:
		levelOpt = level.AllowInfo()
	}

	Logger = level.NewFilter(log.NewLogfmtLogger(LogWriter), levelOpt)
	Logger = log.With(Logger, "time", log.DefaultTimestamp, "caller", log.DefaultCaller)

	if err = LoadConfig(ConfigFilePath); err != nil {
		panic(err)
	}

	LocalAddr = LocalIP + ":" + Cfg.TcpPort

	//PromRegistry.MustRegister(prometheus.NewGoCollector())
}
