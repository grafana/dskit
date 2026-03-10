// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestPolicyHTTPHeaderValue(t *testing.T) {
	tests := []struct {
		name         string
		instanceName string
		policy       *LabelPolicy
	}{
		{
			name:         "single matcher",
			instanceName: "test-instance",
			policy: &LabelPolicy{
				Selector: []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "test_name", "test_value"),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headerVal, err := policyToHeaderValue(tt.instanceName, tt.policy)
			require.NoError(t, err)

			instanceName, policy, err := policyFromHeaderValue(headerVal)
			require.NoError(t, err)
			require.Equal(t, tt.instanceName, instanceName)
			require.Equal(t, len(tt.policy.Selector), len(policy.Selector))
		})
	}

	t.Run("invalid label policies results in error", func(t *testing.T) {
		_, _, err := policyFromHeaderValue("test-instance:xxx/yyy")
		require.Error(t, err)
	})

	t.Run("empty label name in policies results in error", func(t *testing.T) {
		policy := &LabelPolicy{
			Selector: []*labels.Matcher{
				{Type: labels.MatchNotRegexp, Value: "test_value", Name: ""},
			},
		}
		headerVal, err := policyToHeaderValue("test-instance", policy)
		require.NoError(t, err)

		_, _, err = policyFromHeaderValue(headerVal)
		require.Error(t, err)
	})
}

func TestPropagation(t *testing.T) {
	tests := []struct {
		name              string
		instancePolicyMap LabelPolicySet
	}{
		{
			name: "simple",
			instancePolicyMap: LabelPolicySet{
				"test-instance": {
					{Selector: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "test_name", "test_value"),
					}},
				},
			},
		},
		{
			name: "multiple matchers",
			instancePolicyMap: LabelPolicySet{
				"test-instance": {
					{Selector: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "test_name", "test_value"),
						labels.MustNewMatcher(labels.MatchNotEqual, "test_name_alt", "test_value"),
					}},
				},
			},
		},
	}

	requirePolicyMatch := func(t *testing.T, want, got LabelPolicySet) {
		t.Helper()
		require.Equal(t, len(want), len(got))
		for instanceName, policies := range got {
			expectedPolicies, ok := want[instanceName]
			require.True(t, ok)
			require.Equal(t, len(expectedPolicies), len(policies))
			for i, policy := range policies {
				require.Equal(t, len(expectedPolicies[i].Selector), len(policy.Selector))
				for n, matcher := range policy.Selector {
					require.Equal(t, expectedPolicies[i].Selector[n].Type, matcher.Type)
					require.Equal(t, expectedPolicies[i].Selector[n].Name, matcher.Name)
					require.Equal(t, expectedPolicies[i].Selector[n].Value, matcher.Value)
				}
			}
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("HTTP", func(t *testing.T) {
				r := &http.Request{Header: http.Header{}}
				require.NoError(t, InjectLabelMatchersHTTP(r, tt.instancePolicyMap))
				got, err := ExtractLabelMatchersHTTP(r)
				require.NoError(t, err)
				requirePolicyMatch(t, tt.instancePolicyMap, got)
			})

			t.Run("slice", func(t *testing.T) {
				v, err := InjectLabelMatchersSlice(tt.instancePolicyMap)
				require.NoError(t, err)
				got, err := ExtractLabelMatchersSlice(v)
				require.NoError(t, err)
				requirePolicyMatch(t, tt.instancePolicyMap, got)
			})
		})
	}
}

func TestPropagationContext(t *testing.T) {
	lps := LabelPolicySet{
		"test-instance": {
			{Selector: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "test_name", "test_value"),
				labels.MustNewMatcher(labels.MatchNotEqual, "test_name_alt", "test_value"),
			}},
		},
	}

	ctx := InjectLabelMatchersContext(context.Background(), lps)
	got, err := ExtractLabelMatchersContext(ctx)
	require.NoError(t, err)
	require.Equal(t, len(lps), len(got))
	for instanceName, policies := range got {
		expected := lps[instanceName]
		require.Equal(t, len(expected), len(policies))
	}
}

func TestExtractLabelMatchersContext_EmptyContext(t *testing.T) {
	_, err := ExtractLabelMatchersContext(context.Background())
	require.ErrorIs(t, err, errNoMatcherSource)
}

func TestExtractLabelMatchersSlice(t *testing.T) {
	fooBarPolicy := &LabelPolicy{Selector: []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
	}}
	fooCarPolicy := &LabelPolicy{Selector: []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "foo", "car"),
	}}

	tests := []struct {
		name         string
		headerValues []string
		want         LabelPolicySet
		wantErr      error
	}{
		{
			name: "passthrough",
			want: LabelPolicySet{},
		},
		{
			name:         "single-header/single-policy",
			headerValues: []string{"user-1:%7Bfoo=%22bar%22%7D"},
			want:         LabelPolicySet{"user-1": {fooBarPolicy}},
		},
		{
			name:         "single-header/empty-selector",
			headerValues: []string{"user-1:%7B%7D"},
			want:         LabelPolicySet{"user-1": {{Selector: []*labels.Matcher{}}}},
		},
		{
			name:         "single-header/multiple-policies",
			headerValues: []string{"user-1:%7Bfoo=%22bar%22%7D,user-2:%7Bfoo=%22bar%22%7D,user-1:%7Bfoo=%22car%22%7D"},
			want: LabelPolicySet{
				"user-1": {fooBarPolicy, fooCarPolicy},
				"user-2": {fooBarPolicy},
			},
		},
		{
			name: "multiple-header/multiple-policies",
			headerValues: []string{
				"user-1:%7Bfoo=%22bar%22%7D,user-2:%7Bfoo=%22bar%22%7D,user-1:%7Bfoo=%22car%22%7D",
				"user-2:%7Bfoo=%22car%22%7D",
			},
			want: LabelPolicySet{
				"user-1": {fooBarPolicy, fooCarPolicy},
				"user-2": {fooBarPolicy, fooCarPolicy},
			},
		},
		{
			name:         "invalid-header/extra-comma",
			headerValues: []string{"user-1:%7Bfoo=%22bar%22%7D,"},
			wantErr:      errInvalidHeaderParse,
		},
		{
			name:         "invalid-header/no-tenant",
			headerValues: []string{"%7Bfoo=%22bar%22%7D"},
			wantErr:      errInvalidHeaderParse,
		},
		{
			name:         "invalid-header/bad-escape",
			headerValues: []string{"user-1:%%7Bfoo=%22bar%22%7D"},
			wantErr:      errInvalidHeaderEscape,
		},
		{
			name:         "invalid-header/bad-selector",
			headerValues: []string{`user-1:{foo=="bar"}`},
			wantErr:      errInvalidSelector,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("from HTTP headers", func(t *testing.T) {
				r, err := http.NewRequest("GET", "/", nil)
				require.NoError(t, err)
				for _, v := range tt.headerValues {
					r.Header.Add(HTTPHeaderKey, v)
				}
				got, err := ExtractLabelMatchersHTTP(r)
				if tt.wantErr != nil {
					require.ErrorIs(t, err, tt.wantErr)
				} else {
					require.NoError(t, err)
					require.Equal(t, len(tt.want), len(got))
				}
			})

			t.Run("from slice", func(t *testing.T) {
				got, err := ExtractLabelMatchersSlice(tt.headerValues)
				if tt.wantErr != nil {
					require.ErrorIs(t, err, tt.wantErr)
				} else {
					require.NoError(t, err)
					require.Equal(t, len(tt.want), len(got))
				}
			})
		})
	}
}

func TestHash_CollisionResistance(t *testing.T) {
	const instanceCount = 50000
	fooBarPolicy := &LabelPolicy{Selector: []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
	}}
	hashes := make(map[string]struct{}, instanceCount)

	for i := 0; i < instanceCount; i++ {
		lps := LabelPolicySet{fmt.Sprintf("billing%d", i): {fooBarPolicy}}
		hashes[lps.Hash()] = struct{}{}
	}

	require.Equal(t, instanceCount, len(hashes))
}

func TestHash_StructuralCollision(t *testing.T) {
	// [{env="prod"}, {class="open"}] — OR semantics
	orPolicy := LabelPolicySet{"tenant": {
		{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "env", "prod")}},
		{Selector: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "class", "open")}},
	}}

	// [{env="prod", class="open"}] — AND semantics
	andPolicy := LabelPolicySet{"tenant": {
		{Selector: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "env", "prod"),
			labels.MustNewMatcher(labels.MatchEqual, "class", "open"),
		}},
	}}

	require.NotEqual(t, orPolicy.Hash(), andPolicy.Hash(), "OR and AND policies must produce different hashes")
}

func TestLabelPolicySetString(t *testing.T) {
	lps := LabelPolicySet{
		"tenant1": {
			{Selector: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "env", "dev"),
				labels.MustNewMatcher(labels.MatchNotEqual, "level", "info"),
			}},
		},
	}
	s := lps.String()
	require.Contains(t, s, "tenant1:")
	require.Contains(t, s, `env="dev"`)
	require.Contains(t, s, `level!="info"`)
}

func TestInjectMatchers_EmptySet(t *testing.T) {
	req, err := http.NewRequest("GET", "/", nil)
	require.NoError(t, err)
	require.NoError(t, InjectMatchers(req, LabelPolicySet{}))
	require.Empty(t, req.Header.Values(HTTPHeaderKey))
}

func TestInjectMatchers_NonEmpty(t *testing.T) {
	req, err := http.NewRequest("GET", "/", nil)
	require.NoError(t, err)

	lps := LabelPolicySet{
		"tenant1": {{Selector: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "env", "prod"),
		}}},
	}
	require.NoError(t, InjectMatchers(req, lps))
	require.NotEmpty(t, req.Header.Values(HTTPHeaderKey))
}
