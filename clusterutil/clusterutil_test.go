package clusterutil

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestPutClusterIntoOutgoingContext(t *testing.T) {
	ctx := context.Background()
	checkSingleClusterInOutgoingCtx(t, ctx, false, "")

	ctx = PutClusterIntoOutgoingContext(ctx, "my-cluster")
	checkSingleClusterInOutgoingCtx(t, ctx, true, "my-cluster")
}

func TestGetClusterFromIncomingContext(t *testing.T) {
	testCases := map[string]struct {
		incomingContext context.Context
		expectedValue   string
		expectedOutcome bool
	}{
		"no cluster in incoming context gives a false outcome": {
			incomingContext: NewIncomingContext(false, ""),
			expectedOutcome: false,
			expectedValue:   "",
		},
		"non-empty cluster in incoming context gives non-empty string with a true outcome": {
			incomingContext: NewIncomingContext(true, "my-cluster"),
			expectedOutcome: true,
			expectedValue:   "my-cluster",
		},
		"more clusters in incoming context give a false outcome": {
			incomingContext: createContext([]string{"cluster-1", "cluster-2"}),
			expectedOutcome: false,
			expectedValue:   "",
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			value, ok := GetClusterFromIncomingContext(testCase.incomingContext, log.NewNopLogger())
			require.Equal(t, testCase.expectedOutcome, ok)
			require.Equal(t, testCase.expectedValue, value)
		})
	}
}

func checkSingleClusterInOutgoingCtx(t *testing.T, ctx context.Context, shouldExist bool, expectedValue string) {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		require.False(t, ok)

	}
	checkSingleClusterFromMetadata(t, md, shouldExist, expectedValue)
}

func checkSingleClusterFromMetadata(t *testing.T, md metadata.MD, shouldExist bool, expectedValue string) {
	values, ok := md[MetadataClusterVerificationLabelKey]
	if shouldExist {
		require.True(t, ok)
		require.Len(t, values, 1)
		require.Equal(t, expectedValue, values[0])
	} else {
		require.False(t, ok)
	}
}

func createContext(clusters []string) context.Context {
	if len(clusters) == 0 {
		return context.Background()
	}
	md := map[string][]string{
		MetadataClusterVerificationLabelKey: clusters,
	}
	return metadata.NewIncomingContext(context.Background(), md)
}
