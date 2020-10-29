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

package backend

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/baudtime/baudtime/backend/storage"
	"github.com/baudtime/baudtime/backend/visitor"
	"github.com/baudtime/baudtime/meta"
	"github.com/baudtime/baudtime/msg"
	backendmsg "github.com/baudtime/baudtime/msg/backend"
	"github.com/baudtime/baudtime/tcp"
	"github.com/baudtime/baudtime/tcp/client"
	"github.com/baudtime/baudtime/util/syn"
	"github.com/baudtime/baudtime/vars"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/multierr"
	"reflect"
	"sync"
)

type Client interface {
	Select(ctx context.Context, req *backendmsg.SelectRequest) (*backendmsg.SelectResponse, error)
	LabelValues(ctx context.Context, req *backendmsg.LabelValuesRequest) (*msg.LabelValuesResponse, error)
	Add(ctx context.Context, req *backendmsg.AddRequest) error
	Close() error
	Name() string
}

type opMetrics struct {
	activeSel    prometheus.Gauge
	activeAdd    prometheus.Gauge
}

func newOpMetrics(address string) *opMetrics {
	m := &opMetrics{}
	constLabels := prometheus.Labels{
		"addr": address,
	}
	m.activeSel = prometheus.NewGauge(prometheus.GaugeOpts{
		Subsystem:   "gateway",
		Name:        "datanode_active_select_total",
		ConstLabels: constLabels,
	})
	m.activeAdd = prometheus.NewGauge(prometheus.GaugeOpts{
		Subsystem:   "gateway",
		Name:        "datanode_active_add_total",
		ConstLabels: constLabels,
	})

	vars.PromRegistry.MustRegister(
		m.activeSel,
		m.activeAdd,
	)
	return m
}

type metricClient struct {
	*client.Client
	metric *opMetrics
}

type metricClientFactory struct {
	clients sync.Map
}

func (factory *metricClientFactory) getClient(address string) (*metricClient, error) {
	cli, found := factory.clients.Load(address)
	if !found {
		newCli := &metricClient{
			Client: client.NewBackendClient(
				"backend_cli_+"+address, address,
				vars.Cfg.Gateway.ReadConnsPerBackend,
				vars.Cfg.Gateway.WriteConnsPerBackend,
			),
			metric: newOpMetrics(address),
		}
		if cli, found = factory.clients.LoadOrStore(address, newCli); found {
			_ = newCli.Close()
		}
	}

	return cli.(*metricClient), nil
}

func (factory *metricClientFactory) destroy(address string) (err error) {
	cli, found := factory.clients.Load(address)
	if found {
		factory.clients.Delete(address)
		err = cli.(*metricClient).Close()
	}
	return
}

var (
	defaultFactory metricClientFactory
	bytesPool      = syn.NewBucketizedPool(1e3, 1e7, 4, false, func(s int) interface{} { return make([]byte, s) }, func() syn.Bucket {
		return new(sync.Pool)
	})
)

type ShardClient struct {
	shardID      string
	localStorage *storage.Storage
	exeQuery     visitor.Visitor
	codec        tcp.MsgCodec
}

func (c *ShardClient) Select(ctx context.Context, req *backendmsg.SelectRequest) (*backendmsg.SelectResponse, error) {
	if req == nil {
		return nil, nil
	}

	if parentSpan, ok := ctx.Value("span").(opentracing.Span); ok {
		syncRequest := opentracing.StartSpan("syncRequest", opentracing.ChildOf(parentSpan.Context()))
		syncRequest.SetTag("shard", c.shardID)
		for _, m := range req.Matchers {
			syncRequest.SetTag(m.Name, fmt.Sprintf("%s[%s]", m.Value, m.Type.String()))
		}
		defer syncRequest.Finish()

		carrier := new(bytes.Buffer)
		syncRequest.Tracer().Inject(syncRequest.Context(), opentracing.Binary, carrier)
		req.SpanCtx = carrier.Bytes()
	}

	shard, found := meta.GetShard(c.shardID)
	if !found || shard == nil {
		meta.RefreshTopology()
		return nil, errors.Errorf("no such shard %v", c.shardID)
	}

	resp, err := c.exeQuery(shard, func(node *meta.Node) (msg.Message, error) {
		if c.localStorage != nil && node.Addr == vars.LocalAddr {
			return c.localStorage.HandleSelectReq(req), nil
		} else {
			cli, err := defaultFactory.getClient(node.Addr)
			if err != nil {
				return nil, err
			}

			cli.metric.activeSel.Inc()
			resp, err := cli.SyncRequest(ctx, req)
			cli.metric.activeSel.Dec()

			return resp, err
		}
	})

	if err != nil {
		return nil, err
	}

	if selResp, ok := resp.(*backendmsg.SelectResponse); !ok {
		return nil, errors.Wrapf(tcp.BadMsgFormat, "the type of response is '%v'", reflect.TypeOf(resp))
	} else if selResp.Status != msg.StatusCode_Succeed {
		return nil, errors.Errorf("select error:%s", selResp.ErrorMsg)
	} else {
		return selResp, nil
	}
}

func (c *ShardClient) LabelValues(ctx context.Context, req *backendmsg.LabelValuesRequest) (*msg.LabelValuesResponse, error) {
	if req == nil {
		return nil, nil
	}

	if parentSpan, ok := ctx.Value("span").(opentracing.Span); ok {
		syncRequest := opentracing.StartSpan("syncRequest", opentracing.ChildOf(parentSpan.Context()))
		syncRequest.SetTag("shard", c.shardID)
		syncRequest.SetTag("name", req.Name)
		defer syncRequest.Finish()

		carrier := new(bytes.Buffer)
		syncRequest.Tracer().Inject(syncRequest.Context(), opentracing.Binary, carrier)
		req.SpanCtx = carrier.Bytes()
	}

	shard, found := meta.GetShard(c.shardID)
	if !found || shard == nil {
		meta.RefreshTopology()
		return nil, errors.Errorf("no such shard %v", c.shardID)
	}

	resp, err := c.exeQuery(shard, func(node *meta.Node) (msg.Message, error) {
		if c.localStorage != nil && node.Addr == vars.LocalAddr {
			return c.localStorage.HandleLabelValuesReq(req), nil
		} else {
			cli, err := defaultFactory.getClient(node.Addr)
			if err != nil {
				return nil, err
			}

			return cli.SyncRequest(ctx, req)
		}
	})

	if err != nil {
		return nil, err
	}

	if lValsResp, ok := resp.(*msg.LabelValuesResponse); !ok {
		return nil, errors.Wrapf(tcp.BadMsgFormat, "the type of response is '%v'", reflect.TypeOf(resp))
	} else if lValsResp.Status != msg.StatusCode_Succeed {
		return nil, errors.Errorf("label values error:%s", lValsResp.ErrorMsg)
	} else {
		return lValsResp, nil
	}
}

func (c *ShardClient) Add(ctx context.Context, req *backendmsg.AddRequest) (err error) {
	if req == nil {
		return
	}

	master := meta.GetMaster(c.shardID)
	if master != nil {
		if c.localStorage != nil && master.Addr == vars.LocalAddr {
			err = c.localStorage.HandleAddReq(req)
			if err != nil {
				return
			}

			bytes := bytesPool.Get(1 + binary.MaxVarintLen64 + req.Msgsize()).([]byte)

			var n int
			n, err = c.codec.Encode(tcp.Message{Message: req}, bytes)
			if err != nil {
				bytesPool.Put(bytes)
				return
			}

			c.localStorage.ReplicateManager.HandleWriteReq(bytes[:n])
			bytesPool.Put(bytes)
			return
		}

		var cli *metricClient
		if cli, err = defaultFactory.getClient(master.Addr); err == nil {
			if vars.Cfg.Gateway.Appender.AsyncTransfer {
				cli.metric.activeAdd.Inc()
				err = cli.AsyncRequest(req, nil)
				cli.metric.activeAdd.Dec()

				if err == nil {
					return
				}
			} else {
				var resp msg.Message

				cli.metric.activeAdd.Inc()
				resp, err = cli.SyncRequest(ctx, req)
				cli.metric.activeAdd.Dec()

				if err == nil {
					generalResp, ok := resp.(*msg.GeneralResponse)
					if !ok {
						return tcp.BadMsgFormat
					}
					if generalResp.Status == msg.StatusCode_Failed {
						return errors.New(generalResp.Message)
					} else {
						return nil
					}
				}
			}
		}
	}

	meta.FailoverIfNeeded(c.shardID)
	return errors.Wrapf(err, "master not found, may be down? shard id: %s", c.shardID)
}

func (c *ShardClient) Close() error {
	var multiErr error

	master := meta.GetMaster(c.shardID)
	if master != nil {
		err := defaultFactory.destroy(master.Addr)
		if err != nil {
			multiErr = multierr.Append(multiErr, err)
		}
	}

	slaves := meta.GetSlaves(c.shardID)
	if len(slaves) > 0 {
		for _, slave := range slaves {
			err := defaultFactory.destroy(slave.Addr)
			if err != nil {
				multiErr = multierr.Append(multiErr, err)
			}
		}
	}

	return multiErr
}

func (c *ShardClient) Name() string {
	return fmt.Sprintf("[ShardClient]@[shard:%s]", c.shardID)
}
