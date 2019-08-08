package rule

import (
	"context"
	"fmt"
	"github.com/baudtime/baudtime/util"
	htmltemplate "html/template"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/baudtime/baudtime/backend"
	"github.com/baudtime/baudtime/promql"
	"github.com/baudtime/baudtime/util/os/fileutil"
	"github.com/baudtime/baudtime/vars"
	"github.com/fsnotify/fsnotify"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/prometheus/pkg/labels"
)

// QueryFunc processes PromQL queries.
type QueryFunc func(ctx context.Context, q string, t time.Time) (promql.Vector, error)

// EngineQueryFunc returns a new query function that executes instant queries against
// the given engine.
// It converts scaler into vector results.
func EngineQueryFunc(engine *promql.Engine, q backend.Queryable) QueryFunc {
	return func(ctx context.Context, qs string, t time.Time) (promql.Vector, error) {
		q, err := engine.NewInstantQuery(q, qs, t)
		if err != nil {
			return nil, err
		}
		res := q.Exec(ctx)
		if res.Err != nil {
			return nil, res.Err
		}
		switch v := res.Value.(type) {
		case promql.Vector:
			return v, nil
		case promql.Scalar:
			return promql.Vector{promql.Sample{
				Point:  promql.Point(v),
				Metric: labels.Labels{},
			}}, nil
		default:
			return nil, fmt.Errorf("rule result is not a vector or scalar")
		}
	}
}

// A Rule encapsulates a vector expression which is evaluated at a specified
// interval and acted upon (currently either recorded or used for alerting).
type Rule interface {
	Name() string
	// eval evaluates the rule, including any associated recording or alerting actions.
	Eval(context.Context, time.Time, QueryFunc) (promql.Vector, error)
	// String returns a human-readable string representation of the rule.
	String() string

	SetEvaluationTime(time.Duration)
	GetEvaluationTime() time.Duration
	// HTMLSnippet returns a human-readable string representation of the rule,
	// decorated with HTML elements for use the web frontend.
	HTMLSnippet(pathPrefix string) htmltemplate.HTML
}

// Group is a set of rules that have a logical relation.
type Group struct {
	name                 string
	file                 string
	interval             time.Duration
	rules                []Rule
	seriesInPreviousEval []map[string]labels.Labels // One per Rule.
	queryFunc            QueryFunc
	appender             backend.Appender
	evaluationTime       time.Duration
	mtx                  sync.Mutex

	done       chan struct{}
	terminated chan struct{}

	logger log.Logger
}

// NewGroup makes a new Group with the given name, options, and rules.
func NewGroup(name, file string, interval time.Duration, rules []Rule, queryFunc QueryFunc, appender backend.Appender, logger log.Logger) *Group {
	return &Group{
		name:                 name,
		file:                 file,
		interval:             interval,
		rules:                rules,
		queryFunc:            queryFunc,
		appender:             appender,
		seriesInPreviousEval: make([]map[string]labels.Labels, len(rules)),
		done:                 make(chan struct{}),
		terminated:           make(chan struct{}),
		logger:               log.With(logger, "group", name),
	}
}

// Name returns the group name.
func (g *Group) Name() string { return g.name }

// File returns the group's file.
func (g *Group) File() string { return g.file }

// Rules returns the group's rules.
func (g *Group) Rules() []Rule { return g.rules }

func (g *Group) run(ctx context.Context) {
	defer close(g.terminated)

	// Wait an initial amount to have consistently slotted intervals.
	evalTimestamp := g.evalTimestamp().Add(g.interval)
	select {
	case <-time.After(time.Until(evalTimestamp)):
	case <-g.done:
		return
	}

	iter := func() {
		start := time.Now()
		g.Eval(ctx, evalTimestamp)
		evalDuration := time.Since(start)

		g.SetEvaluationTime(evalDuration)
	}

	// The assumption here is that since the ticker was started after having
	// waited for `evalTimestamp` to pass, the ticks will trigger soon
	// after each `evalTimestamp + N * g.interval` occurrence.
	tick := time.NewTicker(g.interval)
	defer tick.Stop()

	iter()
	for {
		select {
		case <-g.done:
			return
		default:
			select {
			case <-g.done:
				return
			case <-tick.C:
				evalTimestamp = time.Now()
				iter()
			}
		}
	}
}

func (g *Group) stop() {
	close(g.done)
	<-g.terminated
}

func (g *Group) hash() uint64 {
	l := labels.New(
		labels.Label{Name: "name", Value: g.name},
		labels.Label{Name: "file", Value: g.file},
	)
	return l.Hash()
}

// GetEvaluationTime returns the time in seconds it took to evaluate the rule group.
func (g *Group) GetEvaluationTime() time.Duration {
	g.mtx.Lock()
	defer g.mtx.Unlock()
	return g.evaluationTime
}

// SetEvaluationTime sets the time in seconds the last evaluation took.
func (g *Group) SetEvaluationTime(dur time.Duration) {
	g.mtx.Lock()
	defer g.mtx.Unlock()
	g.evaluationTime = dur
}

// evalTimestamp returns the immediately preceding consistently slotted evaluation time.
func (g *Group) evalTimestamp() time.Time {
	var (
		offset = int64(g.hash() % uint64(g.interval))
		now    = time.Now().UnixNano()
		adjNow = now - offset
		base   = adjNow - (adjNow % int64(g.interval))
	)

	return time.Unix(0, base+offset)
}

// copyState copies the alerting rule and staleness related state from the given group.
//
// Rules are matched based on their name. If there are duplicates, the
// first is matched with the first, second with the second etc.
func (g *Group) copyState(from *Group) {
	g.evaluationTime = from.evaluationTime

	ruleMap := make(map[string][]int, len(from.rules))

	for fi, fromRule := range from.rules {
		l := ruleMap[fromRule.Name()]
		ruleMap[fromRule.Name()] = append(l, fi)
	}

	for i, rule := range g.rules {
		indexes := ruleMap[rule.Name()]
		if len(indexes) == 0 {
			continue
		}
		fi := indexes[0]
		g.seriesInPreviousEval[i] = from.seriesInPreviousEval[fi]
		ruleMap[rule.Name()] = indexes[1:]
	}
}

// Eval runs a single evaluation cycle in which all rules are evaluated sequentially.
func (g *Group) Eval(ctx context.Context, ts time.Time) {
	for i, rule := range g.rules {
		select {
		case <-g.done:
			return
		default:
		}

		func(i int, rule Rule) {
			defer func(t time.Time) {
				rule.SetEvaluationTime(time.Since(t))
			}(time.Now())

			vector, err := rule.Eval(ctx, ts, g.queryFunc)
			if err != nil {
				// Canceled queries are intentional termination of queries. This normally
				// happens on shutdown and thus we skip logging of any errors here.
				if _, ok := err.(promql.ErrQueryCanceled); !ok {
					level.Warn(g.logger).Log("msg", "Evaluating rule failed", "rule", rule, "err", err)
				}
				return
			}

			seriesReturned := make(map[string]labels.Labels, len(g.seriesInPreviousEval[i]))

			for _, s := range vector {
				lset := util.LabelsToProto(s.Metric)

				if err := g.appender.Add(lset, s.T, s.V, s.Metric.Hash()); err != nil {
					level.Warn(g.logger).Log("msg", "Rule evaluation result discarded", "err", err, "sample", s)
				} else {
					seriesReturned[s.Metric.String()] = s.Metric
				}
			}

			g.seriesInPreviousEval[i] = seriesReturned
		}(i, rule)
	}
}

// The Manager manages recording and alerting rules.
type Manager struct {
	ruleDir        string
	ruleDirWatcher *fsnotify.Watcher
	queryFunc      QueryFunc
	appender       backend.Appender
	groups         map[string]*Group
	mtx            sync.RWMutex
	block          chan struct{}
	ctx            context.Context
	logger         log.Logger
}

// NewManager returns an implementation of Manager, ready to be started
// by calling the Run method.
func NewManager(ctx context.Context, ruleDir string, queryEngine *promql.Engine, api backend.Backend, logger log.Logger) (*Manager, error) {
	app, err := api.Appender()
	if err != nil {
		return nil, err
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	err = watcher.Add(ruleDir)
	if err != nil {
		return nil, err
	}

	return &Manager{
		ruleDir:        ruleDir,
		ruleDirWatcher: watcher,
		queryFunc:      EngineQueryFunc(queryEngine, api),
		appender:       app,
		groups:         map[string]*Group{},
		block:          make(chan struct{}),
		ctx:            ctx,
		logger:         logger,
	}, nil
}

// Run starts processing of the rule manager.
func (m *Manager) Run() {
	if files, err := fileutil.ReadDirNames(m.ruleDir); err == nil {
		m.Update(files)
	}

	go func() {
		for {
			select {
			case event := <-m.ruleDirWatcher.Events:
				level.Info(m.logger).Log("msg", "rule files charged", "event", event)
				if files, err := fileutil.ReadDirNames(m.ruleDir); err == nil {
					err = m.Update(files)
					if err != nil {
						level.Info(m.logger).Log("msg", "update new rule files", "error", err)
					}
				}
			case err := <-m.ruleDirWatcher.Errors:
				level.Error(m.logger).Log("error", err)
			}
		}
	}()

	close(m.block)
}

// Stop the rule manager's rule evaluation cycles.
func (m *Manager) Stop() {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	level.Info(m.logger).Log("msg", "Stopping rule manager...")

	m.ruleDirWatcher.Close()

	for _, eg := range m.groups {
		eg.stop()
	}

	level.Info(m.logger).Log("msg", "Rule manager stopped")
}

// Update the rule manager's state as the config requires. If
// loading the new rules failed the old rule set is restored.
func (m *Manager) Update(files []string) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	// To be replaced with a configurable per-group interval.
	groups, err := m.loadGroups(files...)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup

	for _, newg := range groups {
		wg.Add(1)

		// If there is an old group with the same identifier, stop it and wait for
		// it to finish the current iteration. Then copy it into the new group.
		gn := groupKey(newg.name, newg.file)
		oldg, ok := m.groups[gn]
		delete(m.groups, gn)

		go func(newg *Group) {
			if ok {
				oldg.stop()
				newg.copyState(oldg)
			}
			go func() {
				// Wait with starting evaluation until the rule manager
				// is told to run. This is necessary to avoid running
				// queries against a bootstrapping storage.
				<-m.block
				newg.run(m.ctx)
			}()
			wg.Done()
		}(newg)
	}

	// Stop remaining old groups.
	for _, oldg := range m.groups {
		oldg.stop()
	}

	wg.Wait()
	m.groups = groups

	return nil
}

// loadGroups reads groups from a list of files.
// As there's currently no group syntax a single group named "default" containing
// all rules will be returned.
func (m *Manager) loadGroups(files ...string) (map[string]*Group, error) {
	groups := make(map[string]*Group)

	for _, fn := range files {
		rgs, errs := Parse(filepath.Join(m.ruleDir, fn))
		if errs != nil {
			return nil, errs
		}

		for _, rg := range rgs.GroupDescs {
			itv := time.Duration(vars.Cfg.Gateway.Rule.EvalInterval)
			if rg.Interval != 0 {
				itv = time.Duration(rg.Interval)
			}

			rules := make([]Rule, 0, len(rg.Rules))
			for _, r := range rg.Rules {
				expr, err := promql.ParseExpr(r.Expr)
				if err != nil {
					return nil, err
				}

				rules = append(rules, NewRecordingRule(
					r.Record,
					expr,
					labels.FromMap(r.Labels),
				))
			}

			groups[groupKey(rg.Name, fn)] = NewGroup(rg.Name, fn, itv, rules, m.queryFunc, m.appender, m.logger)
		}
	}

	return groups, nil
}

// Group names need not be unique across filenames.
func groupKey(name, file string) string {
	return name + ";" + file
}

// RuleGroups returns the list of manager's rule groups.
func (m *Manager) RuleGroups() []*Group {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	rgs := make([]*Group, 0, len(m.groups))
	for _, g := range m.groups {
		rgs = append(rgs, g)
	}

	sort.Slice(rgs, func(i, j int) bool {
		return rgs[i].file < rgs[j].file && rgs[i].name < rgs[j].name
	})

	return rgs
}

// Rules returns the list of the manager's rules.
func (m *Manager) Rules() []Rule {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	var rules []Rule
	for _, g := range m.groups {
		rules = append(rules, g.rules...)
	}

	return rules
}