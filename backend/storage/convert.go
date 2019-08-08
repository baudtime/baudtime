package storage

import (
	"github.com/baudtime/baudtime/msg/pb"
	backendpb "github.com/baudtime/baudtime/msg/pb/backend"
	"github.com/prometheus/tsdb/labels"
	"strings"
)

func ProtoToMatchers(matchers []*backendpb.Matcher) ([]labels.Matcher, error) {
	result := make([]labels.Matcher, 0, len(matchers))
	for _, m := range matchers {
		result = append(result, ProtoToMatcher(m))
	}
	return result, nil
}

func ProtoToMatcher(m *backendpb.Matcher) labels.Matcher {
	switch m.Type {
	case backendpb.MatchType_MatchEqual:
		return labels.NewEqualMatcher(m.Name, m.Value)

	case backendpb.MatchType_MatchNotEqual:
		return labels.Not(labels.NewEqualMatcher(m.Name, m.Value))

	case backendpb.MatchType_MatchRegexp:
		res, err := labels.NewRegexpMatcher(m.Name, "^(?:"+m.Value+")$")
		if err != nil {
			panic(err)
		}
		return res

	case backendpb.MatchType_MatchNotRegexp:
		res, err := labels.NewRegexpMatcher(m.Name, "^(?:"+m.Value+")$")
		if err != nil {
			panic(err)
		}
		return labels.Not(res)
	}
	panic("storage.convertMatcher: invalid matcher type")
}

func LabelsToProto(lbs labels.Labels) []pb.Label {
	proto := make([]pb.Label, 0, len(lbs))
	for _, l := range lbs {
		proto = append(proto, pb.Label{Name: l.Name, Value: l.Value})

	}
	return proto
}

func toString(lbs []pb.Label) string {
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
