//go:generate msgp -tests=false

package gateway

import (
	"github.com/baudtime/baudtime/msg"
)

type InstantQueryRequest struct {
	Time    string `msg:"time"`
	Timeout string `msg:"timeout"`
	Query   string `msg:"query"`
}

type RangeQueryRequest struct {
	Start   string `msg:"start"`
	End     string `msg:"end"`
	Step    string `msg:"step"`
	Timeout string `msg:"timeout"`
	Query   string `msg:"query"`
}

type QueryResponse struct {
	Result   string         `msg:"result"`
	Status   msg.StatusCode `msg:"status"`
	ErrorMsg string         `msg:"errorMsg"`
}

type SeriesLabelsRequest struct {
	Matches []string `msg:"matches"`
	Start   string   `msg:"start"`
	End     string   `msg:"end"`
	Timeout string   `msg:"timeout"`
}

type SeriesLabelsResponse struct {
	Labels   [][]msg.Label  `msg:"labels"`
	Status   msg.StatusCode `msg:"status"`
	ErrorMsg string         `msg:"errorMsg"`
}

type LabelValuesRequest struct {
	Name    string   `msg:"name"`
	Matches []string `msg:"matches"`
	Start   string   `msg:"start"`
	End     string   `msg:"end"`
	Timeout string   `msg:"timeout"`
}

//msgp:tuple AddRequest
type AddRequest struct {
	Series   []*msg.Series `msg:"series"`
	Hashed   bool          `msg:"-"`
	HashCode uint64        `msg:"-"`
}

func (r AddRequest) Hashable() (bool, uint64) {
	return r.Hashed, r.HashCode
}
