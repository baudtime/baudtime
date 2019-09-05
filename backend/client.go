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
	"fmt"
	"sync"

	"github.com/baudtime/baudtime/backend/storage"
	"github.com/baudtime/baudtime/meta"
	"github.com/baudtime/baudtime/msg"
	backendmsg "github.com/baudtime/baudtime/msg/backend"
	"github.com/baudtime/baudtime/tcp"
	"github.com/baudtime/baudtime/tcp/client"
	"github.com/baudtime/baudtime/vars"
	"github.com/hashicorp/go-multierror"
	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

type Client interface {
	Select(ctx context.Context, req *backendmsg.SelectRequest) (*backendmsg.SelectResponse, error)
	LabelValues(ctx context.Context, req *backendmsg.LabelValuesRequest) (*msg.LabelValuesResponse, error)
	Add(ctx context.Context, req *backendmsg.AddRequest) error
	Close() error
	Name() string
}

type clientFactory struct {
	clients *sync.Map
}

func (factory *clientFactory) getClient(address string) (*client.Client, error) {
	cli, found := factory.clients.Load(address)
	if !found {
		newCli := client.NewBackendClient("backend_cli_+"+address, address, vars.Cfg.Gateway.ConnNumPerBackend)
		if cli, found = factory.clients.LoadOrStore(address, newCli); found {
			_ = newCli.Close()
		}
	}

	return cli.(*client.Client), nil
}

func (factory *clientFactory) destroy(address string) (err error) {
	cli, found := factory.clients.Load(address)
	if found {
		factory.clients.Delete(address)
		err = cli.(*client.Client).Close()
	}
	return
}

var defaultFactory = &clientFactory{
	clients: new(sync.Map),
}

type ShardClient struct {
	shardID      string
	localStorage *storage.Storage
}

func (c *ShardClient) exeQuery(query func(node *meta.Node) (resp msg.Message, err error)) (resp msg.Message, err error) {
	var multiErr error

	master := meta.GetMaster(c.shardID)

	if master != nil {
		if resp, err = query(master); err != nil {
			multiErr = multierror.Append(multiErr, err)
		} else {
			return
		}
	}

	meta.FailoverIfNeeded(c.shardID)

	slaves := meta.GetSlaves(c.shardID)
	if len(slaves) > 1 {
		for _, node := range slaves[1:] {
			if resp, err = query(node); err != nil {
				multiErr = multierror.Append(multiErr, err)
			} else {
				return
			}
		}
	}

	if multiErr != nil {
		return nil, multiErr
	} else {
		return nil, errors.Errorf("shard %v has no available data node", c.shardID)
	}
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

	resp, err := c.exeQuery(func(node *meta.Node) (msg.Message, error) {
		if c.localStorage != nil && node.IP == vars.LocalIP && node.Port == vars.Cfg.TcpPort {
			if resp := c.localStorage.HandleSelectReq(req); resp.Status != msg.StatusCode_Succeed {
				return nil, errors.Errorf("select error on %s, err:%s", node.Addr(), resp.ErrorMsg)
			} else {
				return resp, nil
			}
		} else {
			cli, err := defaultFactory.getClient(node.Addr())
			if err != nil {
				return nil, err
			}

			resp, err := cli.SyncRequest(ctx, req)
			if err != nil {
				return nil, err
			}

			if _, ok := resp.(*backendmsg.SelectResponse); !ok {
				return nil, tcp.BadMsgTypeError
			}
			return resp, nil
		}
	})

	if err != nil {
		return nil, err
	}
	return resp.(*backendmsg.SelectResponse), nil
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

	resp, err := c.exeQuery(func(node *meta.Node) (msg.Message, error) {
		if c.localStorage != nil && node.IP == vars.LocalIP && node.Port == vars.Cfg.TcpPort {
			if resp := c.localStorage.HandleLabelValuesReq(req); resp.Status != msg.StatusCode_Succeed {
				return nil, errors.Errorf("select error on %s, err:%s", node.Addr(), resp.ErrorMsg)
			} else {
				return resp, nil
			}
		} else {
			cli, err := defaultFactory.getClient(node.Addr())
			if err != nil {
				return nil, err
			}

			resp, err := cli.SyncRequest(ctx, req)
			if err != nil {
				return nil, err
			}

			if _, ok := resp.(*msg.LabelValuesResponse); !ok {
				return nil, tcp.BadMsgTypeError
			}
			return resp, nil
		}
	})
	if err != nil {
		return nil, err
	}
	return resp.(*msg.LabelValuesResponse), nil
}

func (c *ShardClient) Add(ctx context.Context, req *backendmsg.AddRequest) (err error) {
	if req == nil {
		return
	}

	master := meta.GetMaster(c.shardID)
	if master != nil {
		if c.localStorage != nil && master.IP == vars.LocalIP && master.Port == vars.Cfg.TcpPort {
			return c.localStorage.HandleAddReq(req)
		}

		var cli *client.Client
		if cli, err = defaultFactory.getClient(master.Addr()); err == nil {
			if err = cli.AsyncRequest(req, nil); err == nil {
				return
			}
		}
	}

	meta.FailoverIfNeeded(c.shardID)
	return errors.Errorf("master not found, may be down? shard id: %s", c.shardID)
}

func (c *ShardClient) Close() error {
	var multiErr error

	master := meta.GetMaster(c.shardID)
	if master != nil {
		err := defaultFactory.destroy(master.Addr())
		if err != nil {
			multiErr = multierror.Append(multiErr, err)
		}
	}

	slaves := meta.GetSlaves(c.shardID)
	if len(slaves) > 0 {
		for _, slave := range slaves {
			err := defaultFactory.destroy(slave.Addr())
			if err != nil {
				multiErr = multierror.Append(multiErr, err)
			}
		}
	}

	return multiErr
}

func (c *ShardClient) Name() string {
	return fmt.Sprintf("[ShardClient]@[shard:%s]", c.shardID)
}
