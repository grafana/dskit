package clusterutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestPutClusterIntoOutgoingContext(t *testing.T) {
	ctx := context.Background()
	checkSingleClusterInOutgoingCtx(ctx, t, false, "")

	newCtx := PutClusterIntoOutgoingContext(ctx, "")
	checkSingleClusterInOutgoingCtx(newCtx, t, false, "")
	require.Equal(t, ctx, newCtx)

	newCtx = PutClusterIntoOutgoingContext(ctx, "my-cluster")
	checkSingleClusterInOutgoingCtx(newCtx, t, true, "my-cluster")
}

func TestGetClusterFromIncomingContext(t *testing.T) {
	testCases := map[string]struct {
		incomingContext context.Context
		expectedValue   string
		expectedError   error
	}{
		"no cluster in incoming context gives an ErrNoClusterVerificationLabel error": {
			incomingContext: createContext(false, nil),
			expectedError:   ErrNoClusterVerificationLabel,
		},
		"empty cluster in incoming context gives an ErrNoClusterVerificationLabel error": {
			incomingContext: createContext(true, []string{""}),
			expectedError:   ErrNoClusterVerificationLabel,
		},
		"single cluster in incoming context returns that cluster and no errors": {
			incomingContext: createContext(true, []string{"my-cluster"}),
			expectedError:   nil,
			expectedValue:   "my-cluster",
		},
		"more clusters in incoming context give an errDifferentClusterVerificationLabels error": {
			incomingContext: createContext(true, []string{"cluster-1", "cluster-2"}),
			expectedError:   errDifferentClusterVerificationLabels([]string{"cluster-1", "cluster-2"}),
			expectedValue:   "",
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			value, err := GetClusterFromIncomingContext(testCase.incomingContext)
			if testCase.expectedError != nil {
				require.Equal(t, testCase.expectedError, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, testCase.expectedValue, value)
			}
		})
	}
}

func checkSingleClusterInOutgoingCtx(ctx context.Context, t *testing.T, shouldExist bool, expectedValue string) {
	md, ok := metadata.FromOutgoingContext(ctx)
	require.Equal(t, shouldExist, ok)
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

func createContext(containsRequestCluster bool, clusters []string) context.Context {
	ctx := context.Background()
	if !containsRequestCluster {
		return ctx
	}
	if len(clusters) == 0 {
		return context.Background()
	}
	md := map[string][]string{
		MetadataClusterVerificationLabelKey: clusters,
	}
	return metadata.NewIncomingContext(context.Background(), md)
}
