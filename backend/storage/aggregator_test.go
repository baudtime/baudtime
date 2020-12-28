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
	"github.com/baudtime/baudtime/msg"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSum(t *testing.T) {
	initPoints := []msg.Point{
		{T: 1, V: 2},
		{T: 3, V: 6},
		{T: 5, V: 3},
	}

	cases := []struct {
		points   []msg.Point
		expected []msg.Point
	}{
		{
			points: []msg.Point{
				{T: 1, V: 0},
				{T: 3, V: 2},
				{T: 5, V: 1},
				{T: 15, V: 1},
			},
			expected: []msg.Point{
				{T: 1, V: 2},
				{T: 3, V: 8},
				{T: 5, V: 4},
				{T: 15, V: 1},
			},
		},
		{
			points: []msg.Point{
				{T: 1, V: 0},
				{T: 2, V: 2},
				{T: 5, V: 1},
			},
			expected: []msg.Point{
				{T: 1, V: 2},
				{T: 2, V: 2},
				{T: 3, V: 6},
				{T: 5, V: 4},
			},
		},
		{
			points: []msg.Point{
				{T: 2, V: 20},
				{T: 4, V: 40},
			},
			expected: []msg.Point{
				{T: 1, V: 2},
				{T: 2, V: 20},
				{T: 3, V: 6},
				{T: 4, V: 40},
				{T: 5, V: 3},
			},
		},
	}

	sum := new(Sum)

	for _, c := range cases {
		sum.Reset()

		sum.Aggregate(initPoints...)
		sum.Aggregate(c.points...)

		require.Equal(t, c.expected, sum.GetPoints())
	}
}

func TestMin(t *testing.T) {
	initPoints := []msg.Point{
		{T: 1, V: 2},
		{T: 3, V: 6},
		{T: 5, V: 3},
	}

	cases := []struct {
		points   []msg.Point
		expected []msg.Point
	}{
		{
			points: []msg.Point{
				{T: 1, V: 0},
				{T: 3, V: 2},
				{T: 5, V: 1},
			},
			expected: []msg.Point{
				{T: 1, V: 0},
				{T: 3, V: 2},
				{T: 5, V: 1},
			},
		},
		{
			points: []msg.Point{
				{T: 1, V: 3},
				{T: 3, V: 2},
				{T: 5, V: 1},
			},
			expected: []msg.Point{
				{T: 1, V: 2},
				{T: 3, V: 2},
				{T: 5, V: 1},
			},
		},
		{
			points: []msg.Point{
				{T: 1, V: 0},
				{T: 2, V: 2},
				{T: 5, V: 5},
			},
			expected: []msg.Point{
				{T: 1, V: 0},
				{T: 2, V: 2},
				{T: 3, V: 6},
				{T: 5, V: 3},
			},
		},
		{
			points: []msg.Point{
				{T: 2, V: 20},
				{T: 4, V: 40},
				{T: 5, V: 5},
			},
			expected: []msg.Point{
				{T: 1, V: 2},
				{T: 2, V: 20},
				{T: 3, V: 6},
				{T: 4, V: 40},
				{T: 5, V: 3},
			},
		},
	}

	min := new(Min)

	for _, c := range cases {
		min.Reset()

		min.Aggregate(initPoints...)
		min.Aggregate(c.points...)

		require.Equal(t, c.expected, min.GetPoints())
	}
}

func TestMax(t *testing.T) {
	initPoints := []msg.Point{
		{T: 1, V: 2},
		{T: 3, V: 6},
		{T: 5, V: 3},
	}

	cases := []struct {
		points   []msg.Point
		expected []msg.Point
	}{
		{
			points: []msg.Point{
				{T: 1, V: 0},
				{T: 3, V: 2},
				{T: 5, V: 1},
			},
			expected: []msg.Point{
				{T: 1, V: 2},
				{T: 3, V: 6},
				{T: 5, V: 3},
			},
		},
		{
			points: []msg.Point{
				{T: 1, V: 3},
				{T: 3, V: 2},
				{T: 5, V: 1},
			},
			expected: []msg.Point{
				{T: 1, V: 3},
				{T: 3, V: 6},
				{T: 5, V: 3},
			},
		},
		{
			points: []msg.Point{
				{T: 1, V: 0},
				{T: 2, V: 2},
				{T: 5, V: 5},
			},
			expected: []msg.Point{
				{T: 1, V: 2},
				{T: 2, V: 2},
				{T: 3, V: 6},
				{T: 5, V: 5},
			},
		},
		{
			points: []msg.Point{
				{T: 2, V: 20},
				{T: 4, V: 40},
				{T: 5, V: 5},
			},
			expected: []msg.Point{
				{T: 1, V: 2},
				{T: 2, V: 20},
				{T: 3, V: 6},
				{T: 4, V: 40},
				{T: 5, V: 5},
			},
		},
	}


	max := new(Max)

	for _, c := range cases {
		max.Reset()

		max.Aggregate(initPoints...)
		max.Aggregate(c.points...)

		require.Equal(t, c.expected, max.GetPoints())
	}
}
