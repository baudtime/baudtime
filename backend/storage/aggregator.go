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

package storage

import (
	"strings"

	"github.com/baudtime/baudtime/msg"
)

var EmptyPoint = msg.Point{}

type Aggregator interface {
	Aggregate(points ...msg.Point)
	GetLabels() msg.Labels
	GetPoints() []msg.Point
	Reset()
}

type AggregatorCreator func(labels []msg.Label) Aggregator

func GetAggregatorCreator(name string) AggregatorCreator {
	switch strings.ToLower(name) {
	case "sum":
		return func(labels []msg.Label) Aggregator {
			return &Sum{labels: labels}
		}
	case "min":
		return func(labels []msg.Label) Aggregator {
			return &Min{labels: labels}
		}
	case "max":
		return func(labels []msg.Label) Aggregator {
			return &Max{labels: labels}
		}
	}
	return nil
}

//Sum
type Sum struct {
	labels []msg.Label
	points []msg.Point
}

func (sum *Sum) Aggregate(points ...msg.Point) {
	if len(points) == 0 {
		return
	}

	if len(sum.points) == 0 {
		sum.points = append(sum.points, points...)
		return
	}

	i := 0
OUTFOR:
	for _, point := range points {
		for ; i < len(sum.points); i++ {
			if sum.points[i].T > point.T {
				sum.points = append(sum.points[:i+1], sum.points[i:]...)
				sum.points[i] = point
				continue OUTFOR
			} else if sum.points[i].T == point.T {
				sum.points[i].V += point.V
				continue OUTFOR
			}
		}
		sum.points = append(sum.points, point)
	}
}

func (sum *Sum) GetLabels() msg.Labels {
	return sum.labels
}

func (sum *Sum) GetPoints() []msg.Point {
	return sum.points
}

func (sum *Sum) Reset() {
	if sum.points != nil {
		sum.points = sum.points[:0]
	}
}

//Min
type Min struct {
	labels []msg.Label
	points []msg.Point
}

func (min *Min) Aggregate(points ...msg.Point) {
	if len(points) == 0 {
		return
	}

	if len(min.points) == 0 {
		min.points = append(min.points, points...)
		return
	}

	i := 0
OUTFOR:
	for _, point := range points {
		for ; i < len(min.points); i++ {
			if min.points[i].T > point.T {
				min.points = append(min.points[:i+1], min.points[i:]...)
				min.points[i] = point
				continue OUTFOR
			} else if min.points[i].T == point.T {
				if point.V < min.points[i].V {
					min.points[i].V = point.V
				}
				continue OUTFOR
			}
		}
		min.points = append(min.points, point)
	}
}

func (min *Min) GetLabels() msg.Labels {
	return min.labels
}

func (min *Min) GetPoints() []msg.Point {
	return min.points
}

func (min *Min) Reset() {
	if min.points != nil {
		min.points = min.points[:0]
	}
}

//Max
type Max struct {
	labels []msg.Label
	points []msg.Point
}

func (max *Max) Aggregate(points ...msg.Point) {
	if len(points) == 0 {
		return
	}

	if len(max.points) == 0 {
		max.points = append(max.points, points...)
		return
	}

	i := 0
OUTFOR:
	for _, point := range points {
		for ; i < len(max.points); i++ {
			if max.points[i].T > point.T {
				max.points = append(max.points[:i+1], max.points[i:]...)
				max.points[i] = point
				continue OUTFOR
			} else if max.points[i].T == point.T {
				if point.V > max.points[i].V {
					max.points[i].V = point.V
				}
				continue OUTFOR
			}
		}
		max.points = append(max.points, point)
	}
}

func (max *Max) GetLabels() msg.Labels {
	return max.labels
}

func (max *Max) GetPoints() []msg.Point {
	return max.points
}

func (max *Max) Reset() {
	if max.points != nil {
		max.points = max.points[:0]
	}
}
