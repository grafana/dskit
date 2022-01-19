package flagext_test

import (
	"bytes"
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/flagext"
)

func TestRenamedFlag(t *testing.T) {
	for _, tc := range []struct {
		name  string
		flags []string
		check func(t *testing.T, c *configWithRenamedFlags, err error, logged string, deprecatedFlagsIncrease int)
	}{
		{
			name:  "old int value",
			flags: []string{"-old-int=10"},
			check: func(t *testing.T, c *configWithRenamedFlags, err error, logged string, deprecatedFlagsIncrease int) {
				require.NoError(t, err)
				assert.Equal(t, 10, c.Int)
				assert.Equal(t, `level=warn msg="flag renamed" flag=old-int new_flag_name=new-int`, strings.TrimSpace(logged))
				testutil.ToFloat64(flagext.DeprecatedFlagsUsed)
				assert.Equal(t, 1, deprecatedFlagsIncrease)
			},
		},
		{
			name:  "old int value twice",
			flags: []string{"-old-int=10", "-old-int=20"},
			check: func(t *testing.T, c *configWithRenamedFlags, err error, logged string, deprecatedFlagsIncrease int) {
				require.NoError(t, err)
				assert.Equal(t, 20, c.Int)
				assert.Equal(t, `level=warn msg="flag renamed" flag=old-int new_flag_name=new-int`, strings.TrimSpace(logged))
				assert.Equal(t, 2, deprecatedFlagsIncrease)
			},
		},
		{
			name:  "new int value",
			flags: []string{"-new-int=10"},
			check: func(t *testing.T, c *configWithRenamedFlags, err error, logged string, deprecatedFlagsIncrease int) {
				require.NoError(t, err)
				assert.Equal(t, 10, c.Int)
				assert.Empty(t, logged)
				assert.Equal(t, 0, deprecatedFlagsIncrease)
			},
		},
		{
			name:  "new int value twice",
			flags: []string{"-new-int=10", "-new-int=20"},
			check: func(t *testing.T, c *configWithRenamedFlags, err error, logged string, deprecatedFlagsIncrease int) {
				require.NoError(t, err)
				assert.Equal(t, 20, c.Int)
				assert.Empty(t, logged)
				assert.Equal(t, 0, deprecatedFlagsIncrease)
			},
		},
		{
			name:  "default int value",
			flags: []string{},
			check: func(t *testing.T, c *configWithRenamedFlags, err error, logged string, deprecatedFlagsIncrease int) {
				require.NoError(t, err)
				assert.Equal(t, 5, c.Int)
				assert.Empty(t, logged)
				assert.Equal(t, 0, deprecatedFlagsIncrease)
			},
		},
		{
			name:  "both new int64 and old int64 defined",
			flags: []string{"-old-int=10", "-new-int=10"},
			check: func(t *testing.T, c *configWithRenamedFlags, err error, logged string, deprecatedFlagsIncrease int) {
				require.Error(t, err)
				assert.Contains(t, err.Error(), `invalid value "10" for flag -new-int: flag old-int was renamed to new-int, but both old and new names were used, please use only the new name`)
				assert.Equal(t, 1, deprecatedFlagsIncrease)
			},
		},

		{
			name:  "old int64 value",
			flags: []string{"-old-int64=10"},
			check: func(t *testing.T, c *configWithRenamedFlags, err error, logged string, deprecatedFlagsIncrease int) {
				require.NoError(t, err)
				assert.Equal(t, int64(10), c.Int64)
				assert.Equal(t, `level=warn msg="flag renamed" flag=old-int64 new_flag_name=new-int64`, strings.TrimSpace(logged))
				assert.Equal(t, 1, deprecatedFlagsIncrease)
			},
		},
		{
			name:  "new int64 value",
			flags: []string{"-new-int64=10"},
			check: func(t *testing.T, c *configWithRenamedFlags, err error, logged string, deprecatedFlagsIncrease int) {
				require.NoError(t, err)
				assert.Equal(t, int64(10), c.Int64)
				assert.Empty(t, logged)
				assert.Equal(t, 0, deprecatedFlagsIncrease)
			},
		},
		{
			name:  "default int64 value",
			flags: []string{},
			check: func(t *testing.T, c *configWithRenamedFlags, err error, logged string, deprecatedFlagsIncrease int) {
				require.NoError(t, err)
				assert.Equal(t, int64(5), c.Int64)
				assert.Empty(t, logged)
				assert.Equal(t, 0, deprecatedFlagsIncrease)
			},
		},
		{
			name:  "both new int64 and old int64 defined",
			flags: []string{"-old-int64=10", "-new-int64=10"},
			check: func(t *testing.T, c *configWithRenamedFlags, err error, logged string, deprecatedFlagsIncrease int) {
				require.Error(t, err)
				assert.Contains(t, err.Error(), `invalid value "10" for flag -new-int64: flag old-int64 was renamed to new-int64, but both old and new names were used, please use only the new name`)
				assert.Equal(t, 1, deprecatedFlagsIncrease)
			},
		},

		// For the rest of the flags we don't check the erroring part etc since it doesn't depend on the flag type, so we only test using the old value
		{
			name:  "old string",
			flags: []string{"-old-string=foo"},
			check: func(t *testing.T, c *configWithRenamedFlags, err error, logged string, deprecatedFlagsIncrease int) {
				require.NoError(t, err)
				assert.Equal(t, "foo", c.String)
				assert.Equal(t, `level=warn msg="flag renamed" flag=old-string new_flag_name=new-string`, strings.TrimSpace(logged))
				assert.Equal(t, 1, deprecatedFlagsIncrease)
			},
		},
		{
			name:  "old bool",
			flags: []string{"-old-bool"},
			check: func(t *testing.T, c *configWithRenamedFlags, err error, logged string, deprecatedFlagsIncrease int) {
				require.NoError(t, err)
				assert.Equal(t, true, c.Bool)
				assert.Equal(t, `level=warn msg="flag renamed" flag=old-bool new_flag_name=new-bool`, strings.TrimSpace(logged))
				assert.Equal(t, 1, deprecatedFlagsIncrease)
			},
		},
		{
			name:  "old duration",
			flags: []string{"-old-duration=5m"},
			check: func(t *testing.T, c *configWithRenamedFlags, err error, logged string, deprecatedFlagsIncrease int) {
				require.NoError(t, err)
				assert.Equal(t, 5*time.Minute, c.Duration)
				assert.Equal(t, `level=warn msg="flag renamed" flag=old-duration new_flag_name=new-duration`, strings.TrimSpace(logged))
				assert.Equal(t, 1, deprecatedFlagsIncrease)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logged := bytes.NewBuffer(nil)
			logger := log.NewLogfmtLogger(log.NewSyncWriter(logged))

			f := flag.NewFlagSet("test", flag.ContinueOnError)
			cfg := &configWithRenamedFlags{}
			cfg.RegisterFlags(f, logger)

			deprecatedFlagsCountBefore := int(testutil.ToFloat64(flagext.DeprecatedFlagsUsed))
			err := f.Parse(tc.flags)
			deprecatedFlagsIncrease := int(testutil.ToFloat64(flagext.DeprecatedFlagsUsed)) - deprecatedFlagsCountBefore

			tc.check(t, cfg, err, logged.String(), deprecatedFlagsIncrease)
		})
	}

}

type configWithRenamedFlags struct {
	Int      int
	Int64    int64
	Bool     bool
	String   string
	Duration time.Duration
}

func (c *configWithRenamedFlags) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	flagext.RenamedFlag(f, logger, flagext.NewIntValue(5, &c.Int),
		"old-int", "Old description for old-int flag, use new-int now please",
		"new-int", "Brand new int value",
	)
	flagext.RenamedFlag(f, logger, flagext.NewInt64Value(5, &c.Int64),
		"old-int64", "Old description for old-int64 flag, use new-int64 now please",
		"new-int64", "Brand new int64 value",
	)
	flagext.RenamedFlag(f, logger, flagext.NewBoolValue(false, &c.Bool),
		"old-bool", "Old description for old-bool flag, use new-bool now please",
		"new-bool", "Brand new bool value",
	)
	flagext.RenamedFlag(f, logger, flagext.NewStringValue("bar", &c.String),
		"old-string", "Old description for old-string flag, use new-string now please",
		"new-string", "Brand new string value",
	)
	flagext.RenamedFlag(f, logger, flagext.NewDurationValue(time.Hour, &c.Duration),
		"old-duration", "Old description for old-duration flag, use new-duration now please",
		"new-duration", "Brand new duration value",
	)
}
