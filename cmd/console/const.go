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

var helpCommands = [][]string{
	{"SLAVEOF", "host port", "Replication"},
	{"INSTANTQRY", "expression [timestamp]", "10bit unix timestamp is the number of seconds that have elapsed since 00:00:00 Coordinated Universal Time (UTC), Thursday, 1 January 1970"},
	{"WRITEPOINT", "metric{l=v, l=v} value timestamp", ""},
	{"LABELVALS", "name start end", "Server"},
	{"JOINCLUSTER", "-", "Server"},
	{"INFO", "-", "Server"},
	{"PING", "-", "Server"},
}
