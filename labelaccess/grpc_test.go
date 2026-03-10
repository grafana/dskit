// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestInjectExtractGRPC_RoundTrip(t *testing.T) {
	lps := LabelPolicySet{
		"tenant1": {{Selector: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "env", "prod"),
		}}},
		"tenant2": {{Selector: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchNotEqual, "level", "debug"),
			labels.MustNewMatcher(labels.MatchRegexp, "region", "us-.*"),
		}}},
	}

	// Inject into outgoing context (simulates client interceptor).
	outCtx := InjectLabelMatchersContext(context.Background(), lps)
	outCtx, err := injectIntoGRPCRequest(outCtx)
	require.NoError(t, err)

	// Simulate the metadata crossing the wire: move outgoing → incoming.
	outMD, ok := metadata.FromOutgoingContext(outCtx)
	require.True(t, ok)
	inCtx := metadata.NewIncomingContext(context.Background(), outMD)

	// Extract on the server side (simulates server interceptor).
	inCtx, err = extractFromGRPCRequest(inCtx)
	require.NoError(t, err)

	extracted, err := ExtractLabelMatchersContext(inCtx)
	require.NoError(t, err)
	require.Equal(t, len(lps), len(extracted))
	for tenant, policies := range extracted {
		expected := lps[tenant]
		require.Equal(t, len(expected), len(policies))
		for i, p := range policies {
			require.Equal(t, len(expected[i].Selector), len(p.Selector))
			for j, m := range p.Selector {
				require.Equal(t, expected[i].Selector[j].Type, m.Type)
				require.Equal(t, expected[i].Selector[j].Name, m.Name)
				require.Equal(t, expected[i].Selector[j].Value, m.Value)
			}
		}
	}
}

func TestInjectIntoGRPCRequest_NoPolicies(t *testing.T) {
	// Context without policies: injectIntoGRPCRequest must return ctx unchanged (no error).
	ctx, err := injectIntoGRPCRequest(context.Background())
	require.NoError(t, err)
	_, ok := metadata.FromOutgoingContext(ctx)
	require.False(t, ok, "no outgoing metadata should be set when there are no policies")
}

func TestExtractFromGRPCRequest_NoMetadata(t *testing.T) {
	// Context without metadata: extractFromGRPCRequest must return ctx unchanged.
	ctx, err := extractFromGRPCRequest(context.Background())
	require.NoError(t, err)
	_, err = ExtractLabelMatchersContext(ctx)
	require.ErrorIs(t, err, errNoMatcherSource)
}
