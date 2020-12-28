//go:generate msgp -tests=false

package msg

import (
	"encoding/json"
	"strings"
)

type StatusCode byte

const (
	StatusCode_Succeed StatusCode = iota
	StatusCode_Failed
)

//msgp:tuple Label
type Label struct {
	Name  string `msg:"name"`
	Value string `msg:"value"`
}

//msgp:tuple Point
type Point struct {
	T int64   `msg:"T"`
	V float64 `msg:"V"`
}

type Labels []Label

func (ls Labels) MarshalJSON() ([]byte, error) {
	return json.Marshal(ls.Map())
}

func (ls Labels) Map() map[string]string {
	m := make(map[string]string, len(ls))
	for _, l := range ls {
		m[l.Name] = l.Value
	}
	return m
}

func Compare(a, b Labels) int {
	l := len(a)
	if len(b) < l {
		l = len(b)
	}

	for i := 0; i < l; i++ {
		if d := strings.Compare(a[i].Name, b[i].Name); d != 0 {
			return d
		}
		if d := strings.Compare(a[i].Value, b[i].Value); d != 0 {
			return d
		}
	}
	// If all labels so far were in common, the set with fewer labels comes first.
	return len(a) - len(b)
}

//msgp:tuple Series
type Series struct {
	Labels []Label `msg:"labels"`
	Points []Point `msg:"points"`
}

type LabelValuesResponse struct {
	Values   []string   `msg:"values"`
	Status   StatusCode `msg:"status"`
	ErrorMsg string     `msg:"errorMsg"`
}

type GeneralResponse struct {
	Status  StatusCode `msg:"status"`
	Message string     `msg:"message"`
}
