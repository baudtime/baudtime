package rule

import (
	"github.com/baudtime/baudtime/promql"
	"github.com/baudtime/baudtime/util/toml"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"go.uber.org/multierr"
)

// descErr represents semantical errors on parsing rule groups.
type descErr struct {
	Group    string
	Rule     int
	RuleName string
	Err      error
}

func (err *descErr) Error() string {
	return errors.Wrapf(err.Err, "group %q, rule %d, %q", err.Group, err.Rule, err.RuleName).Error()
}

// RuleGroups is a set of rule groups that are typically exposed in a file.
type RuleGroupDescs struct {
	GroupDescs []RuleGroupDesc `toml:"groups"`
}

func (descs *RuleGroupDescs) Validate() (err error) {
	set := map[string]struct{}{}

	for _, g := range descs.GroupDescs {
		if g.Name == "" {
			err = multierr.Append(err, errors.Errorf("group name should not be empty"))
		}

		if _, ok := set[g.Name]; ok {
			err = multierr.Append(err, errors.Errorf("group name: \"%s\" is repeated in the same file", g.Name))
		}

		set[g.Name] = struct{}{}

		for i, r := range g.Rules {
			for _, err := range r.Validate() {
				ruleName := r.Record
				err = multierr.Append(err, &descErr{
					Group:    g.Name,
					Rule:     i,
					RuleName: ruleName,
					Err:      err,
				})
			}
		}
	}

	return
}

// RuleGroupDesc is a list of sequentially evaluated recording and alerting rules.
type RuleGroupDesc struct {
	Name     string        `toml:"name"`
	Interval toml.Duration `toml:"interval,omitempty"`
	Rules    []RuleDesc    `toml:"rules"`
}

// Rule describes an alerting or recording rule.
type RuleDesc struct {
	Record string            `toml:"record"`
	Expr   string            `toml:"expr"`
	Labels map[string]string `toml:"labels,omitempty"`
}

// Validate the rule and return a list of encountered errors.
func (desc *RuleDesc) Validate() (errs []error) {
	if desc.Record == "" {
		errs = append(errs, errors.Errorf("'record' must be set"))
	}

	if !model.IsValidMetricName(model.LabelValue(desc.Record)) {
		errs = append(errs, errors.Errorf("invalid recording rule name: %s", desc.Record))
	}

	if desc.Expr == "" {
		errs = append(errs, errors.Errorf("field 'expr' must be set in rule"))
	} else if _, err := promql.ParseExpr(desc.Expr); err != nil {
		errs = append(errs, errors.Errorf("could not parse expression: %s", err))
	}

	for k, v := range desc.Labels {
		if !model.LabelName(k).IsValid() {
			errs = append(errs, errors.Errorf("invalid label name: %s", k))
		}

		if !model.LabelValue(v).IsValid() {
			errs = append(errs, errors.Errorf("invalid label value: %s", v))
		}
	}

	return errs
}

// ParseFile reads and parses rules from a file.
func Parse(descFile string) (*RuleGroupDescs, error) {
	var groupDescs RuleGroupDescs
	err := toml.LoadFromToml(descFile, &groupDescs)
	if err != nil {
		return nil, err
	}

	return &groupDescs, groupDescs.Validate()
}
