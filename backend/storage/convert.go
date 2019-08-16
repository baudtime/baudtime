package storage

import (
	"github.com/baudtime/baudtime/msg"
	backendmsg "github.com/baudtime/baudtime/msg/backend"
	"github.com/prometheus/tsdb/labels"
	"strings"
)

func ProtoToMatchers(matchers []*backendmsg.Matcher) ([]labels.Matcher, error) {
	result := make([]labels.Matcher, 0, len(matchers))
	for _, m := range matchers {
		result = append(result, ProtoToMatcher(m))
	}
	return result, nil
}

func ProtoToMatcher(m *backendmsg.Matcher) labels.Matcher {
	switch m.Type {
	case backendmsg.MatchType_MatchEqual:
		return labels.NewEqualMatcher(m.Name, m.Value)

	case backendmsg.MatchType_MatchNotEqual:
		return labels.Not(labels.NewEqualMatcher(m.Name, m.Value))

	case backendmsg.MatchType_MatchRegexp:
		res, err := labels.NewRegexpMatcher(m.Name, "^(?:"+m.Value+")$")
		if err != nil {
			panic(err)
		}
		return res

	case backendmsg.MatchType_MatchNotRegexp:
		res, err := labels.NewRegexpMatcher(m.Name, "^(?:"+m.Value+")$")
		if err != nil {
			panic(err)
		}
		return labels.Not(res)
	}
	panic("storage.convertMatcher: invalid matcher type")
}

func LabelsToProto(lbs labels.Labels) []msg.Label {
	proto := make([]msg.Label, 0, len(lbs))
	for _, l := range lbs {
		proto = append(proto, msg.Label{Name: l.Name, Value: l.Value})

	}
	return proto
}

func toString(lbs []msg.Label) string {
	var b strings.Builder

	b.WriteByte('{')
	for i, l := range lbs {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(l.Name)
		b.WriteByte('=')
		b.WriteString(l.Value)
	}
	b.WriteByte('}')

	return b.String()
}
