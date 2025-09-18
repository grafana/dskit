package clusterutil

import (
	"flag"
	"fmt"

	"github.com/grafana/dskit/flagext"
)

type ClusterValidationConfig struct {
	Label           string                  `yaml:"label" category:"experimental"`
	Labels          flagext.StringSliceCSV  `yaml:"labels" category:"experimental"`
	registeredFlags flagext.RegisteredFlags `yaml:"-"`
}

func (cfg *ClusterValidationConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.registeredFlags = flagext.TrackRegisteredFlags(prefix, f, func(prefix string, f *flag.FlagSet) {
		f.StringVar(&cfg.Label, prefix+"label", "", "Cluster validation label (deprecated, use labels instead).")
		f.Var(&cfg.Labels, prefix+"labels", "Comma-separated list of cluster validation labels.")
	})
}

func (cfg *ClusterValidationConfig) RegisteredFlags() flagext.RegisteredFlags {
	return cfg.registeredFlags
}

// GetEffectiveLabels returns the effective cluster validation labels.
// For backwards compatibility, if the deprecated Label field is used, it returns it as a single-element slice.
// If both Label and Labels are set, it returns an error during validation.
func (cfg *ClusterValidationConfig) GetEffectiveLabels() []string {
	if cfg.Label != "" {
		return []string{cfg.Label}
	}
	return cfg.Labels
}

// Validate ensures that Label and Labels are not both set.
func (cfg *ClusterValidationConfig) Validate() error {
	if cfg.Label != "" && len(cfg.Labels) > 0 {
		return fmt.Errorf("cluster validation label and labels cannot both be set - use labels instead of the deprecated label flag")
	}
	return nil
}

type ServerClusterValidationConfig struct {
	ClusterValidationConfig `yaml:",inline"`
	GRPC                    ClusterValidationProtocolConfig        `yaml:"grpc" category:"experimental"`
	HTTP                    ClusterValidationProtocolConfigForHTTP `yaml:"http" category:"experimental"`
	registeredFlags         flagext.RegisteredFlags                `yaml:"-"`
}

func (cfg *ServerClusterValidationConfig) Validate() error {
	// First validate the base cluster validation config
	if err := cfg.ClusterValidationConfig.Validate(); err != nil {
		return err
	}

	// Get the effective labels (either from deprecated Label or new Labels)
	labels := cfg.GetEffectiveLabels()

	err := cfg.GRPC.Validate("grpc", labels)
	if err != nil {
		return err
	}
	return cfg.HTTP.Validate("http", labels)
}

func (cfg *ServerClusterValidationConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.registeredFlags = flagext.TrackRegisteredFlags(prefix, f, func(prefix string, f *flag.FlagSet) {
		cfg.ClusterValidationConfig.RegisterFlagsWithPrefix(prefix, f)
		cfg.GRPC.RegisterFlagsWithPrefix(prefix+"grpc.", f)
		cfg.HTTP.RegisterFlagsWithPrefix(prefix+"http.", f)
	})
}

func (cfg *ServerClusterValidationConfig) RegisteredFlags() flagext.RegisteredFlags {
	return cfg.registeredFlags
}

type ClusterValidationProtocolConfig struct {
	Enabled        bool `yaml:"enabled" category:"experimental"`
	SoftValidation bool `yaml:"soft_validation" category:"experimental"`
}

func (cfg *ClusterValidationProtocolConfig) Validate(prefix string, labels []string) error {
	if len(labels) == 0 {
		if cfg.Enabled || cfg.SoftValidation {
			return fmt.Errorf("%s: validation cannot be enabled if cluster validation labels are not configured", prefix)
		}
		return nil
	}

	if !cfg.Enabled && cfg.SoftValidation {
		return fmt.Errorf("%s: soft validation can be enabled only if cluster validation is enabled", prefix)
	}
	return nil
}

func (cfg *ClusterValidationProtocolConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	softValidationFlag := prefix + "soft-validation"
	enabledFlag := prefix + "enabled"
	f.BoolVar(&cfg.SoftValidation, softValidationFlag, false, fmt.Sprintf("When enabled, soft cluster label validation is executed. Can be enabled only together with %s", enabledFlag))
	f.BoolVar(&cfg.Enabled, enabledFlag, false, "When enabled, cluster label validation is executed: configured cluster validation label is compared with the cluster validation label received through the requests.")
}

type ClusterValidationProtocolConfigForHTTP struct {
	ClusterValidationProtocolConfig `yaml:",inline"`
	ExcludedPaths                   flagext.StringSliceCSV `yaml:"excluded_paths" category:"experimental"`
	ExcludedUserAgents              flagext.StringSliceCSV `yaml:"excluded_user_agents" category:"experimental"`
}

func (cfg *ClusterValidationProtocolConfigForHTTP) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.ClusterValidationProtocolConfig.RegisterFlagsWithPrefix(prefix, f)
	f.Var(&cfg.ExcludedPaths, prefix+"excluded-paths", "Comma-separated list of url paths that are excluded from the cluster validation check.")
	f.Var(&cfg.ExcludedUserAgents, prefix+"excluded-user-agents", "Comma-separated list of user agents that are excluded from the cluster validation check.")
}
