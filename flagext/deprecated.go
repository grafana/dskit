package flagext

import (
	"flag"
	"fmt"

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

type deprecatedFlag struct {
	name   string
	logger log.Logger
}

func (deprecatedFlag) String() string {
	return "deprecated"
}

func (d deprecatedFlag) Set(string) error {
	level.Warn(d.logger).Log("msg", "flag disabled", "flag", d.name)
	DeprecatedFlagsUsed.Inc()
	return nil
}

// DeprecatedFlag logs a warning when you try to use it.
func DeprecatedFlag(f *flag.FlagSet, name, message string, logger log.Logger) {
	f.Var(deprecatedFlag{name: name, logger: logger}, name, message)
}

// RenamedFlag looks up the new flag and registers the old version of it, which would also be usable,
// but it would warn and increment the deprecated flags metric.
func RenamedFlag(f *flag.FlagSet, logger log.Logger, oldName, newName string) error {
	fl := f.Lookup(newName)
	if fl == nil {
		return fmt.Errorf("flag with name %s is not registered, so can't register old version %s", newName, oldName)
	}
	value := fl.Value

	oldValue := &renamedOldFlag{name: oldName, v: value, logger: logger}
	newValue := &renamedNewFlag{name: newName, v: value}
	oldValue.new, newValue.old = newValue, oldValue

	fl.Value = newValue
	f.Var(oldValue, oldName, fmt.Sprintf("%s (deprecated, renamed to %s)", fl.Usage, fl.Name))
	return nil
}

// MustRenameFlag does the same as RenamedFlag but it panics instead of returning an error.
func MustRenameFlag(f *flag.FlagSet, logger log.Logger, oldName, newName string) {
	if err := RenamedFlag(f, logger, oldName, newName); err != nil {
		panic(err)
	}
}

// boolFlag is used to identify boolean flags, see flag/flag.go from stdlib.
type boolFlag interface {
	flag.Value
	IsBoolFlag() bool
}

type renamedOldFlag struct {
	name string
	v    flag.Value

	new *renamedNewFlag
	set bool

	logger log.Logger
}

func (r *renamedOldFlag) String() string {
	if r.new == nil {
		// we need to check r.new because flag.isZeroValue calls String() on a zero-value created by reflection.
		return ""
	}
	return fmt.Sprintf("deprecated, use %s instead", r.new.name)
}

func (r *renamedOldFlag) Set(s string) error {
	if r.new.set {
		return fmt.Errorf("flag %s was renamed to %s, but both old and new names were used, please use only the new name", r.name, r.new.name)
	}
	if !r.set {
		level.Warn(r.logger).Log("msg", "flag renamed", "flag", r.name, "new_flag_name", r.new.name)
	}

	r.set = true
	DeprecatedFlagsUsed.Inc()
	return r.v.Set(s)
}

// IsBoolFlag implements boolFlag and is needed for flag package to properly identify the underlying bool flag.
func (r *renamedOldFlag) IsBoolFlag() bool {
	if bf, ok := r.v.(boolFlag); ok {
		return bf.IsBoolFlag()
	}
	return false
}

type renamedNewFlag struct {
	name string
	v    flag.Value

	old *renamedOldFlag
	set bool
}

func (r *renamedNewFlag) String() string {
	if r.v == nil {
		// we need to check r.v because flag.isZeroValue calls String() on a zero-value created by reflection.
		return ""
	}
	return r.v.String()
}

func (r *renamedNewFlag) Set(s string) error {
	if r.old.set {
		return fmt.Errorf("flag %s was renamed to %s, but both old and new names were used, please use only the new name", r.old.name, r.name)
	}
	r.set = true
	return r.v.Set(s)
}

// IsBoolFlag implements boolFlag and is needed for flag package to properly identify the underlying bool flag.
func (r *renamedNewFlag) IsBoolFlag() bool {
	if bf, ok := r.v.(boolFlag); ok {
		return bf.IsBoolFlag()
	}
	return false
}
