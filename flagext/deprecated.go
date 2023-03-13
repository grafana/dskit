package flagext

import (
	"encoding"
	"flag"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// DeprecatedFlagsUsed is the metric that counts deprecated flags set.
var DeprecatedFlagsUsed = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "deprecated_flags_inuse_total",
		Help: "The number of deprecated flags currently set.",
	})

// DisabledFlagsUsed is the metric that counts deprecated flags set.
var DisabledFlagsUsed = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "disabled_flags_inuse_total",
		Help: "The number of disabled flags currently set.",
	})

type disabledFlag struct {
	name              string
	countAsDeprecated bool
	logger            log.Logger
}

func (disabledFlag) String() string {
	return "deprecated"
}

func (d disabledFlag) Set(string) error {
	level.Warn(d.logger).Log("msg", "flag disabled", "flag", d.name)
	if d.countAsDeprecated {
		DeprecatedFlagsUsed.Inc()
	} else {
		DisabledFlagsUsed.Inc()
	}
	return nil
}

// DeprecatedFlag registers a noop flag and logs a warning when you try to use it and increments the DeprecatedFlagsUsed counter.
//
// This function is deprecated and will be removed in the future. Please use DisabledFlag or the DeprecatedVar alternatives.
func DeprecatedFlag(f *flag.FlagSet, name, message string, logger log.Logger) {
	f.Var(disabledFlag{name: name, logger: logger, countAsDeprecated: true}, name, message)
}

// DisabledFlag registers a noop flag and logs a warning when you try to use it and increments the DisabledFlagsUsed counter.
func DisabledFlag(f *flag.FlagSet, name, message string, logger log.Logger) {
	f.Var(disabledFlag{name: name, logger: logger, countAsDeprecated: false}, name, message)
}

// deprecatedFlag wraps a flag.FlagSet so we can add behaviour on Set without having to reimplement the parsing.
type deprecatedFlag struct {
	name     string
	logger   log.Logger
	delegate *flag.FlagSet
}

func (d deprecatedFlag) String() (s string) {
	// The delegate should only have a single registered flag.
	d.delegate.VisitAll(func(f *flag.Flag) {
		s = f.Value.String()
	})
	return
}

func (d deprecatedFlag) Set(s string) error {
	level.Warn(d.logger).Log("msg", "flag deprecated", "flag", d.name)
	DeprecatedFlagsUsed.Inc()
	return d.delegate.Set(d.name, s)
}

// DeprecatedIntVar registers a flag the same as its stdlib flag counterpart. If the flag is set, DeprecatedIntVar will
// log the usage as a warning on the provided logger and will increment DeprecatedFlagsUsed.
func DeprecatedIntVar(f *flag.FlagSet, val *int, name string, defaultVal int, usage string, logger log.Logger) {
	delegateSet := flag.NewFlagSet(name, flag.ContinueOnError)
	delegateSet.IntVar(val, name, defaultVal, usage)
	f.Var(deprecatedFlag{
		name:     name,
		logger:   logger,
		delegate: delegateSet,
	}, name, usage)
}

// DeprecatedInt64Var registers a flag the same as its stdlib flag counterpart. If the flag is set, DeprecatedInt64Var will
// log the usage as a warning on the provided logger and will increment DeprecatedFlagsUsed.
func DeprecatedInt64Var(f *flag.FlagSet, val *int64, name string, defaultVal int64, usage string, logger log.Logger) {
	delegateSet := flag.NewFlagSet(name, flag.ContinueOnError)
	delegateSet.Int64Var(val, name, defaultVal, usage)
	f.Var(deprecatedFlag{
		name:     name,
		logger:   logger,
		delegate: delegateSet,
	}, name, usage)
}

// DeprecatedDurationVar registers a flag the same as its stdlib flag counterpart. If the flag is set, DeprecatedDurationVar will
// log the usage as a warning on the provided logger and will increment DeprecatedFlagsUsed.
func DeprecatedDurationVar(f *flag.FlagSet, val *time.Duration, name string, defaultVal time.Duration, usage string, logger log.Logger) {
	delegateSet := flag.NewFlagSet(name, flag.ContinueOnError)
	delegateSet.DurationVar(val, name, defaultVal, usage)
	f.Var(deprecatedFlag{
		name:     name,
		logger:   logger,
		delegate: delegateSet,
	}, name, usage)
}

// DeprecatedBoolVar registers a flag the same as its stdlib flag counterpart. If the flag is set, DeprecatedBoolVar will
// log the usage as a warning on the provided logger and will increment DeprecatedFlagsUsed.
func DeprecatedBoolVar(f *flag.FlagSet, val *bool, name string, defaultVal bool, usage string, logger log.Logger) {
	delegateSet := flag.NewFlagSet(name, flag.ContinueOnError)
	delegateSet.BoolVar(val, name, defaultVal, usage)
	f.Var(deprecatedFlag{
		name:     name,
		logger:   logger,
		delegate: delegateSet,
	}, name, usage)
}

// DeprecatedUintVar registers a flag the same as its stdlib flag counterpart. If the flag is set, DeprecatedUintVar will
// log the usage as a warning on the provided logger and will increment DeprecatedFlagsUsed.
func DeprecatedUintVar(f *flag.FlagSet, val *uint, name string, defaultVal uint, usage string, logger log.Logger) {
	delegateSet := flag.NewFlagSet(name, flag.ContinueOnError)
	delegateSet.UintVar(val, name, defaultVal, usage)
	f.Var(deprecatedFlag{
		name:     name,
		logger:   logger,
		delegate: delegateSet,
	}, name, usage)
}

// DeprecatedUint64Var registers a flag the same as its stdlib flag counterpart. If the flag is set, DeprecatedUint64Var will
// log the usage as a warning on the provided logger and will increment DeprecatedFlagsUsed.
func DeprecatedUint64Var(f *flag.FlagSet, val *uint64, name string, defaultVal uint64, usage string, logger log.Logger) {
	delegateSet := flag.NewFlagSet(name, flag.ContinueOnError)
	delegateSet.Uint64Var(val, name, defaultVal, usage)
	f.Var(deprecatedFlag{
		name:     name,
		logger:   logger,
		delegate: delegateSet,
	}, name, usage)
}

// DeprecatedFloat64Var registers a flag the same as its stdlib flag counterpart. If the flag is set, DeprecatedFloat64Var will
// log the usage as a warning on the provided logger and will increment DeprecatedFlagsUsed.
func DeprecatedFloat64Var(f *flag.FlagSet, val *float64, name string, defaultVal float64, usage string, logger log.Logger) {
	delegateSet := flag.NewFlagSet(name, flag.ContinueOnError)
	delegateSet.Float64Var(val, name, defaultVal, usage)
	f.Var(deprecatedFlag{
		name:     name,
		logger:   logger,
		delegate: delegateSet,
	}, name, usage)
}

// DeprecatedStringVar registers a flag the same as its stdlib flag counterpart. If the flag is set, DeprecatedStringVar will
// log the usage as a warning on the provided logger and will increment DeprecatedFlagsUsed.
func DeprecatedStringVar(f *flag.FlagSet, val *string, name string, defaultVal string, usage string, logger log.Logger) {
	delegateSet := flag.NewFlagSet(name, flag.ContinueOnError)
	delegateSet.StringVar(val, name, defaultVal, usage)
	f.Var(deprecatedFlag{
		name:     name,
		logger:   logger,
		delegate: delegateSet,
	}, name, usage)
}

// DeprecatedTextVar registers a flag the same as its stdlib flag counterpart. If the flag is set, DeprecatedTextVar will
// log the usage as a warning on the provided logger and will increment DeprecatedFlagsUsed.
func DeprecatedTextVar(f *flag.FlagSet, val encoding.TextUnmarshaler, name string, defaultVal encoding.TextMarshaler, usage string, logger log.Logger) {
	delegateSet := flag.NewFlagSet(name, flag.ContinueOnError)
	delegateSet.TextVar(val, name, defaultVal, usage)
	f.Var(deprecatedFlag{
		name:     name,
		logger:   logger,
		delegate: delegateSet,
	}, name, usage)
}

// DeprecatedVar registers a flag the same as its stdlib flag counterpart. If the flag is set, DeprecatedVar will
// log the usage as a warning on the provided logger and will increment DeprecatedFlagsUsed.
func DeprecatedVar(f *flag.FlagSet, v flag.Value, name string, usage string, logger log.Logger) {
	delegateSet := flag.NewFlagSet(name, flag.ContinueOnError)
	delegateSet.Var(v, name, usage)
	f.Var(deprecatedFlag{
		name:     name,
		logger:   logger,
		delegate: delegateSet,
	}, name, usage)
}
