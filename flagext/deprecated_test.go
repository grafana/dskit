package flagext_test

import (
	"bytes"
	"flag"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/flagext"
)

func TestRenamedFlag(t *testing.T) {
	type configStruct struct {
		Int  int
		Bool bool
	}
	registerFlags := func(cfg *configStruct, f *flag.FlagSet, logger log.Logger) {
		f.IntVar(&cfg.Int, "new-int", 5, "Brand new int value.")
		flagext.MustRenameFlag(f, logger, "old-int", "new-int")
		f.BoolVar(&cfg.Bool, "new-bool", false, "Brand new bool value.")
		flagext.MustRenameFlag(f, logger, "old-bool", "new-bool")
	}

	for _, tc := range []struct {
		name  string
		flags []string
		check func(t *testing.T, c *configStruct, err error, logged string, deprecatedFlagsIncrease int)
	}{
		{
			name:  "old int value",
			flags: []string{"-old-int=10"},
			check: func(t *testing.T, c *configStruct, err error, logged string, deprecatedFlagsIncrease int) {
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
			check: func(t *testing.T, c *configStruct, err error, logged string, deprecatedFlagsIncrease int) {
				require.NoError(t, err)
				assert.Equal(t, 20, c.Int)
				assert.Equal(t, `level=warn msg="flag renamed" flag=old-int new_flag_name=new-int`, strings.TrimSpace(logged))
				assert.Equal(t, 2, deprecatedFlagsIncrease)
			},
		},
		{
			name:  "new int value",
			flags: []string{"-new-int=10"},
			check: func(t *testing.T, c *configStruct, err error, logged string, deprecatedFlagsIncrease int) {
				require.NoError(t, err)
				assert.Equal(t, 10, c.Int)
				assert.Empty(t, logged)
				assert.Equal(t, 0, deprecatedFlagsIncrease)
			},
		},
		{
			name:  "new int value twice",
			flags: []string{"-new-int=10", "-new-int=20"},
			check: func(t *testing.T, c *configStruct, err error, logged string, deprecatedFlagsIncrease int) {
				require.NoError(t, err)
				assert.Equal(t, 20, c.Int)
				assert.Empty(t, logged)
				assert.Equal(t, 0, deprecatedFlagsIncrease)
			},
		},
		{
			name:  "default int value",
			flags: []string{},
			check: func(t *testing.T, c *configStruct, err error, logged string, deprecatedFlagsIncrease int) {
				require.NoError(t, err)
				assert.Equal(t, 5, c.Int)
				assert.Empty(t, logged)
				assert.Equal(t, 0, deprecatedFlagsIncrease)
			},
		},
		{
			name:  "old bool",
			flags: []string{"-old-bool"},
			check: func(t *testing.T, c *configStruct, err error, logged string, deprecatedFlagsIncrease int) {
				require.NoError(t, err)
				assert.Equal(t, true, c.Bool)
				assert.Equal(t, `level=warn msg="flag renamed" flag=old-bool new_flag_name=new-bool`, strings.TrimSpace(logged))
				assert.Equal(t, 1, deprecatedFlagsIncrease)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logged := bytes.NewBuffer(nil)
			logger := log.NewLogfmtLogger(log.NewSyncWriter(logged))

			f := flag.NewFlagSet("test", flag.ContinueOnError)

			cfg := &configStruct{}
			registerFlags(cfg, f, logger)

			deprecatedFlagsCountBefore := int(testutil.ToFloat64(flagext.DeprecatedFlagsUsed))
			err := f.Parse(tc.flags)
			deprecatedFlagsIncrease := int(testutil.ToFloat64(flagext.DeprecatedFlagsUsed)) - deprecatedFlagsCountBefore

			tc.check(t, cfg, err, logged.String(), deprecatedFlagsIncrease)
		})
	}

	t.Run("rename non-defined flag", func(t *testing.T) {
		f := flag.NewFlagSet("test", flag.ContinueOnError)
		err := flagext.RenamedFlag(f, log.NewNopLogger(), "foo", "bar")
		require.Error(t, err)
	})
}
