//go:generate msgp -tests=false

package msg

import "encoding/json"

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
