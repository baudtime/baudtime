/*
 * Copyright 2019 The Baudtime Authors
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"

	"github.com/baudtime/baudtime"
	"github.com/baudtime/baudtime/util"
	"github.com/baudtime/baudtime/vars"
	"github.com/go-kit/kit/log/level"
	"github.com/opentracing/opentracing-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	vars.Init("baudtime")

	if vars.CpuProfile != "" {
		f, err := os.Create(vars.CpuProfile)
		if err != nil {
			level.Error(vars.Logger).Log("msg", "can't open cpu profile file", "err", err)
			return
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	defer func() {
		if x := recover(); x != nil {
			level.Error(vars.Logger).Log("panic", x)
			vars.LogWriter.Write(debug.Stack())
		}

		if vars.MemProfile != "" {
			f, err := os.Create(vars.MemProfile)
			if err != nil {
				level.Error(vars.Logger).Log("msg", "can't open memory profile file", "err", err)
				return
			}
			runtime.GC()
			if err := pprof.WriteHeapProfile(f); err != nil {
				level.Error(vars.Logger).Log("msg", "can't write memory profile file", "err", err)
			}
			f.Close()
		}
	}()

	if vars.Cfg.Jaeger != nil {
		jaegerCfg := jaegercfg.Configuration{
			ServiceName: vars.Cfg.NameSpace,
			Sampler: &jaegercfg.SamplerConfig{
				Type:  vars.Cfg.Jaeger.SamplerType,
				Param: float64(vars.Cfg.Jaeger.SampleNumPerSec),
			},
			Reporter: &jaegercfg.ReporterConfig{
				CollectorEndpoint: vars.Cfg.Jaeger.CollectorEndpoint,
			},
		}
		tracer, closer, err := jaegerCfg.NewTracer(
			jaegercfg.Logger(&util.Jaegerlogger{vars.Logger}),
		)
		if err != nil {
			level.Error(vars.Logger).Log("msg", "Could not initialize jaeger tracer", "err", err)
			return
		}
		opentracing.SetGlobalTracer(tracer)

		defer closer.Close()
	}

	baudtime.Run()
}
