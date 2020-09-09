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
	"context"
	"encoding/json"
	"fmt"
	"github.com/baudtime/baudtime"
	"github.com/baudtime/baudtime/msg"
	backendmsg "github.com/baudtime/baudtime/msg/backend"
	"github.com/baudtime/baudtime/promql"
	ts "github.com/baudtime/baudtime/util/time"
	"github.com/pkg/errors"
	"strconv"
	"strings"
	"time"
)

type queryResult struct {
	ResultType promql.ValueType `json:"resultType"`
	Result     promql.Value     `json:"result"`
}

type executor struct {
	addr        string
	codedConn   *CodedConn
	queryEngine *promql.Engine
	closed      bool
}

func (e *executor) execCommand(args []string) error {
	if len(args) == 0 {
		return nil
	}

	cmd := args[0]
	if len(args) > 1 {
		args = args[1:]
	} else {
		args = nil
	}

	switch cmd {
	case "help", "?":
		printHelp(args)
	case "quit", "exit":
		fmt.Println("Bye bye^_^")
		e.codedConn.Close()
		e.closed = true
	case "joincluster":
		if len(args) != 0 {
			printCommandHelp(cmd)
			return nil
		}

		return e.execComand(&backendmsg.AdminCmdJoinCluster{})
	case "leftcluster":
		if len(args) != 0 {
			printCommandHelp(cmd)
			return nil
		}

		return e.execComand(&backendmsg.AdminCmdLeftCluster{})
	case "info":
		if len(args) != 0 {
			printCommandHelp(cmd)
			return nil
		}

		return e.execComand(&backendmsg.AdminCmdInfo{})
	case "slaveof":
		if len(args) != 2 {
			printCommandHelp(cmd)
			return nil
		}

		var command *backendmsg.SlaveOfCommand

		if args[0] == "no" && args[1] == "one" {
			command = &backendmsg.SlaveOfCommand{}
		} else {
			command = &backendmsg.SlaveOfCommand{
				MasterAddr: fmt.Sprintf("%s:%s", args[0], args[1]),
			}
		}

		return e.execComand(command)
	case "instantqry":
		if len(args) != 1 && len(args) != 2 {
			printCommandHelp(cmd)
			return nil
		}

		expression := strings.Replace(args[0], " ", "", -1)
		ts := time.Now()

		if len(args) == 2 {
			var err error
			ts, err = baudtime.ParseTime(args[1])
			if err != nil {
				fmt.Print(err)
				return err
			}
		}

		qry, err := e.queryEngine.NewInstantQuery(QueryableConn(e.codedConn), expression, ts)
		if err != nil {
			fmt.Print(err)
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
		res := qry.Exec(ctx)
		cancel()
		if res.Err != nil {
			fmt.Print(res.Err)
			return res.Err
		}

		queryRes, err := json.MarshalIndent(&queryResult{
			ResultType: res.Value.Type(),
			Result:     res.Value,
		}, "", "\t")
		if err != nil {
			fmt.Print(err)
			return err
		}

		fmt.Println(string(queryRes))
	case "writepoint":
		if len(args) != 2 && len(args) != 3 {
			printCommandHelp(cmd)
			return nil
		}

		var labels []msg.Label

		labelStr := strings.Replace(args[0], " ", "", -1)
		labelStr = strings.Replace(labelStr, "\"", "", -1)

		labelBytes := []byte(labelStr)
		idx1 := strings.Index(labelStr, "{")
		idx2 := strings.Index(labelStr, "}")

		metricName := string(labelBytes[:idx1])
		labels = append(labels, msg.Label{
			Name:  "__name__",
			Value: metricName,
		})

		labelStr = string(labelBytes[idx1+1 : idx2])
		if len(labelStr) > 0 {
			pairs := strings.Split(labelStr, ",")
			for _, p := range pairs {
				array := strings.Split(p, "=")
				labels = append(labels, msg.Label{
					Name:  strings.Trim(array[0], " "),
					Value: strings.Trim(array[1], " "),
				})
			}
		}

		var t int64
		if len(args) == 3 {
			var err error
			t, err = strconv.ParseInt(args[2], 10, 0)
			if err != nil {
				fmt.Print(err)
				return err
			}
		} else {
			t = ts.FromTime(time.Now())
		}

		v, err := strconv.ParseFloat(args[1], 64)
		if err != nil {
			fmt.Print(err)
			return err
		}

		series := &msg.Series{
			Labels: labels,
			Points: []msg.Point{{T: t, V: v}},
		}

		addRequest := &backendmsg.AddRequest{
			Series: []*msg.Series{series},
		}

		err = e.codedConn.WriteRaw(addRequest)
		if err != nil {
			fmt.Println(err.Error())
			return err
		}
	case "labelvals":
		if len(args) != 3 {
			printCommandHelp(cmd)
			return nil
		}

		mint, err := strconv.ParseInt(args[1], 10, 0)
		if err != nil {
			fmt.Print(err)
			return err
		}

		maxt, err := strconv.ParseInt(args[2], 10, 0)
		if err != nil {
			fmt.Print(err)
			return err
		}

		return e.execComand(&backendmsg.LabelValuesRequest{
			Name: args[0],
			Mint: mint,
			Maxt: maxt,
		})
	default:
		fmt.Println("Unkown Command")
	}

	return nil
}

func (e *executor) execComand(cmd msg.Message) error {
	if cmd != nil {
		err := e.codedConn.WriteRaw(cmd)
		if err != nil {
			fmt.Println(err.Error())
			return err
		}

		reply, err := e.codedConn.ReadRaw()
		if err != nil {
			fmt.Println(err.Error())
			return err
		}

		if reply == nil {
			fmt.Print("(nil)")
			return errors.New("reply is nil")
		}

		switch r := reply.(type) {
		case *msg.GeneralResponse:
			if r.Status == msg.StatusCode_Succeed {
				if r.Message != "" {
					fmt.Println(r.Message)
				} else {
					fmt.Println("ok")
				}
			} else {
				fmt.Println("error occurred!", r.Message)
				return errors.New(r.Message)
			}
		case *msg.LabelValuesResponse:
			if r.Status == msg.StatusCode_Succeed {
				fmt.Println(r.Values)
			} else {
				fmt.Println(r.ErrorMsg)
				return errors.New(r.ErrorMsg)
			}
		default:
			fmt.Print("invalid reply")
			return errors.New("invalid reply")
		}
	}
	return nil
}

func (e *executor) reconnect() (err error) {
	e.codedConn, err = NewCodedConn(e.addr)
	return
}
