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

package baudtime

import (
	"context"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/baudtime/baudtime/backend"
	"github.com/baudtime/baudtime/backend/storage"
	"github.com/baudtime/baudtime/meta"
	"github.com/baudtime/baudtime/msg"
	backendmsg "github.com/baudtime/baudtime/msg/backend"
	gatewaymsg "github.com/baudtime/baudtime/msg/gateway"
	"github.com/baudtime/baudtime/promql"
	"github.com/baudtime/baudtime/rule"
	"github.com/baudtime/baudtime/tcp"
	osutil "github.com/baudtime/baudtime/util/os"
	. "github.com/baudtime/baudtime/vars"
	"github.com/buaazp/fasthttprouter"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/tsdb"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/pprofhandler"
)

type tcpServerObserver struct {
	gateway   *Gateway
	storage   *storage.Storage
	heartbeat *meta.Heartbeat
}

func (obs *tcpServerObserver) OnStart() error {
	if obs.gateway != nil {
		if err := meta.Init(); err != nil {
			level.Error(Logger).Log("msg", "failed to init meta data", "err", err)
			return err
		}
	}
	if obs.heartbeat != nil {
		if err := obs.heartbeat.Start(); err != nil {
			level.Error(Logger).Log("msg", "failed to start heartbeat", "err", err)
			return err
		}
	}
	level.Info(Logger).Log("msg", "baudtime started")
	return nil
}

func (obs *tcpServerObserver) OnStop() error {
	if obs.heartbeat != nil {
		obs.heartbeat.Stop()
		obs.storage.Close()
	}
	level.Info(Logger).Log("msg", "baudtime shutdown")
	return nil
}

func (obs *tcpServerObserver) OnAccept(tcpConn *net.TCPConn) *tcp.ReadWriteLoop {
	level.Debug(Logger).Log("msg", "new connection accepted", "remoteAddr", tcpConn.RemoteAddr())

	tcpConn.SetNoDelay(true)
	tcpConn.SetKeepAlive(true)
	tcpConn.SetKeepAlivePeriod(60 * time.Second)
	tcpConn.SetReadBuffer(1024 * 1024)
	tcpConn.SetWriteBuffer(1024 * 1024)

	return tcp.NewReadWriteLoop(tcpConn, func(ctx context.Context, req tcp.Message, reqBytes []byte) tcp.Message {
		raw := req.GetRaw()
		response := tcp.Message{Opaque: req.GetOpaque()}

		switch request := raw.(type) {
		case *gatewaymsg.AddRequest:
			err := obs.gateway.Ingest(request)
			if err != nil {
				response.SetRaw(&msg.GeneralResponse{
					Status:  msg.StatusCode_Failed,
					Message: err.Error(),
				})
			} else {
				response.SetRaw(&msg.GeneralResponse{
					Status: msg.StatusCode_Succeed,
				})
			}
		case *gatewaymsg.InstantQueryRequest:
			response.SetRaw(obs.gateway.InstantQuery(request))
		case *gatewaymsg.RangeQueryRequest:
			response.SetRaw(obs.gateway.RangeQuery(request))
		case *gatewaymsg.LabelValuesRequest:
			response.SetRaw(obs.gateway.LabelValues(request))
		case *backendmsg.AddRequest:
			err := obs.storage.HandleAddReq(request)
			obs.storage.ReplicateManager.HandleWriteReq(reqBytes)
			if err != nil {
				response.SetRaw(&msg.GeneralResponse{
					Status:  msg.StatusCode_Failed,
					Message: err.Error(),
				})
			} else {
				return tcp.EmptyMsg
			}
		case *backendmsg.SelectRequest:
			response.SetRaw(obs.storage.HandleSelectReq(request))
		case *backendmsg.LabelValuesRequest:
			response.SetRaw(obs.storage.HandleLabelValuesReq(request))
		case *backendmsg.SlaveOfCommand:
			response.SetRaw(obs.storage.ReplicateManager.HandleSlaveOfCmd(request))
		case *backendmsg.SyncHandshake:
			response.SetRaw(obs.storage.ReplicateManager.HandleSyncHandshake(request))
		case *backendmsg.SyncHeartbeat:
			response.SetRaw(obs.storage.ReplicateManager.HandleHeartbeat(request))
		case *backendmsg.AdminCmdInfo:
			info, err := obs.storage.Info(true)
			if err != nil {
				response.SetRaw(&msg.GeneralResponse{Status: msg.StatusCode_Failed, Message: err.Error()})
			} else {
				response.SetRaw(&msg.GeneralResponse{Status: msg.StatusCode_Succeed, Message: info.String()})
			}
		case *backendmsg.AdminCmdJoinCluster:
			obs.storage.ReplicateManager.JoinCluster()
			response.SetRaw(&msg.GeneralResponse{Status: msg.StatusCode_Succeed, Message: obs.storage.ReplicateManager.RelationID()})
		}

		return response
	})
}

func Run() {
	var (
		localStorage *storage.Storage
		heartbeat    *meta.Heartbeat
		gateway      *Gateway
		router       = fasthttprouter.New()
	)

	if Cfg.Storage != nil {
		walSegmentSize := 0
		if !Cfg.Storage.TSDB.EnableWal {
			walSegmentSize = -1
		}

		db, err := tsdb.Open(Cfg.Storage.TSDB.Path, Logger, nil, &tsdb.Options{
			WALSegmentSize:         walSegmentSize,
			RetentionDuration:      uint64(Cfg.Storage.TSDB.RetentionDuration) / 1e6,
			BlockRanges:            Cfg.Storage.TSDB.BlockRanges,
			NoLockfile:             Cfg.Storage.TSDB.NoLockfile,
			AllowOverlappingBlocks: true,
		})
		if err != nil {
			level.Error(Logger).Log("msg", "failed to open db", "err", err)
			return
		}

		localStorage = storage.New(db)
		heartbeat = meta.NewHeartbeat(time.Duration(Cfg.Storage.StatReport.SessionExpireTTL), time.Duration(Cfg.Storage.StatReport.HeartbeartInterval), func() (meta.Node, error) {
			stat, err := localStorage.Info(false)
			return stat.Node, err
		})

		router.GET("/joinCluster", func(ctx *fasthttp.RequestCtx) {
			localStorage.ReplicateManager.JoinCluster()
		})
		router.GET("/stat", func(ctx *fasthttp.RequestCtx) {
			if arg := ctx.QueryArgs().Peek("reset"); arg != nil {
				localStorage.OpStat.Reset()
			}
			stat, err := localStorage.Info(true)
			if err != nil {
				ctx.Error(err.Error(), http.StatusInternalServerError)
			} else {
				ctx.SuccessString("text/plain", stat.String())
			}
		})
		router.GET("/dump", func(ctx *fasthttp.RequestCtx) {
			dir := "/tmp/baudtime/dump"
			if arg := ctx.QueryArgs().Peek("dir"); arg != nil {
				dir = string(arg)
			}
			localStorage.DB.Snapshot(dir, true)
		})
	}

	if Cfg.Gateway != nil {
		promql.LookbackDelta = time.Duration(Cfg.LookbackDelta)
		fanout := backend.NewFanout(localStorage)
		queryEngine := promql.NewEngine(nil, Cfg.Gateway.QueryEngine.Concurrency, time.Duration(Cfg.Gateway.QueryEngine.Timeout))

		if Cfg.Gateway.Rule != nil && Cfg.Gateway.Rule.RuleFileDir == "" {
			ruleManager, err := rule.NewManager(context.Background(), Cfg.Gateway.Rule.RuleFileDir, queryEngine, fanout, Logger)
			if err != nil {
				level.Error(Logger).Log("msg", "failed to init rule manager", "err", err)
				return
			}

			ruleManager.Run()
			defer ruleManager.Stop()
		}

		gateway = &Gateway{
			Backend:     fanout,
			QueryEngine: queryEngine,
		}

		router.GET("/api/v1/query", gateway.HttpInstantQuery)
		router.POST("/api/v1/query", gateway.HttpInstantQuery)
		router.GET("/api/v1/query_range", gateway.HttpRangeQuery)
		router.POST("/api/v1/query_range", gateway.HttpRangeQuery)
		router.GET("/api/v1/label/:name/values", gateway.HttpLabelValues)

		for _, suffix := range []string{"", Base64Suffix} {
			jobBase64Encoded := suffix == Base64Suffix
			router.POST("/metrics/job"+suffix+"/:job/*labels", gateway.HttpIngest(jobBase64Encoded))
			router.PUT("/metrics/job"+suffix+"/:job/*labels", gateway.HttpIngest(jobBase64Encoded))
			router.POST("/metrics/job"+suffix+"/:job", gateway.HttpIngest(jobBase64Encoded))
			router.PUT("/metrics/job"+suffix+"/:job", gateway.HttpIngest(jobBase64Encoded))
		}
	}

	httpServer := &fasthttp.Server{}
	go func() {
		httpServer.Handler = func(ctx *fasthttp.RequestCtx) {
			if strings.HasPrefix(string(ctx.Path()), "/debug/pprof") {
				pprofhandler.PprofHandler(ctx)
			} else {
				router.Handler(ctx)
			}
		}
		if err := httpServer.ListenAndServe(":" + Cfg.HttpPort); err != nil {
			level.Error(Logger).Log("msg", "failed to start http server for baudtime", "err", err)
			return
		}
	}()

	tcpServer := tcp.NewTcpServer(Cfg.TcpPort, Cfg.MaxConn, &tcpServerObserver{
		gateway:   gateway,
		storage:   localStorage,
		heartbeat: heartbeat,
	})
	go tcpServer.Run()

	osutil.HandleSignals(func(sig os.Signal) bool {
		level.Warn(Logger).Log("msg", "trapped signal", "signal", sig)
		if sig == syscall.SIGTERM || sig == syscall.SIGINT {
			go httpServer.Shutdown()
			tcpServer.Shutdown()
			return false
		}
		return true
	})
}
