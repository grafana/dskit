package clusterutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestPutClusterIntoOutgoingContext(t *testing.T) {
	ctx := context.Background()
	checkSingleClusterInOutgoingCtx(t, ctx, false, "")

	newCtx := PutClusterIntoOutgoingContext(ctx, "")
	checkSingleClusterInOutgoingCtx(t, newCtx, false, "")
	require.Equal(t, ctx, newCtx)

	newCtx = PutClusterIntoOutgoingContext(ctx, "my-cluster")
	checkSingleClusterInOutgoingCtx(t, newCtx, true, "my-cluster")
}

func TestGetClusterFromIncomingContext(t *testing.T) {
	testCases := map[string]struct {
		incomingContext context.Context
		failOnEmpty     bool
		expectedValue   string
		expectedError   error
	}{
		"no cluster in incoming context gives an ErrNoClusterVerificationLabel error": {
			incomingContext: NewIncomingContext(false, ""),
			failOnEmpty:     true,
			expectedError:   ErrNoClusterVerificationLabel,
		},
		"single cluster in incoming context returns that cluster and no errors": {
			incomingContext: NewIncomingContext(true, "my-cluster"),
			expectedError:   nil,
			expectedValue:   "my-cluster",
		},
		"more clusters in incoming context give an errDifferentClusterVerificationLabels error": {
			incomingContext: createContext([]string{"cluster-1", "cluster-2"}),
			expectedError:   NewErrDifferentClusterVerificationLabels([]string{"cluster-1", "cluster-2"}),
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
