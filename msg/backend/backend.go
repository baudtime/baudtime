//go:generate msgp -tests=false

package backend

import (
	"github.com/baudtime/baudtime/msg"
)

type MatchType byte

const (
	MatchType_MatchEqual MatchType = iota
	MatchType_MatchNotEqual
	MatchType_MatchRegexp
	MatchType_MatchNotRegexp
)

func (z MatchType) String() string {
	switch z {
	case MatchType_MatchEqual:
		return "Equal"
	case MatchType_MatchNotEqual:
		return "NotEqual"
	case MatchType_MatchRegexp:
		return "Regexp"
	case MatchType_MatchNotRegexp:
		return "NotRegexp"
	}
	return "<Invalid>"
}

type Matcher struct {
	Type  MatchType `msg:"Type"`
	Name  string    `msg:"Name"`
	Value string    `msg:"Value"`
}

type SelectRequest struct {
	Mint       int64      `msg:"mint"`
	Maxt       int64      `msg:"maxt"`
	Step       int64      `msg:"step"`
	Func       string     `msg:"func"`
	Grouping   []string   `msg:"grouping"`
	By         bool       `msg:"by"`
	Matchers   []*Matcher `msg:"matchers"`
	OnlyLabels bool       `msg:"onlyLB"`
	SpanCtx    []byte     `msg:"spanCtx"`
}

type SelectResponse struct {
	Status   msg.StatusCode `msg:"status"`
	Series   []*msg.Series  `msg:"series"`
	ErrorMsg string         `msg:"errorMsg"`
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

type LabelValuesRequest struct {
	Mint     int64      `msg:"mint"`
	Maxt     int64      `msg:"maxt"`
	Name     string     `msg:"name"`
	Matchers []*Matcher `msg:"matchers"`
	SpanCtx  []byte     `msg:"spanCtx"`
}
