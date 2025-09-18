package clusterutil

import (
	"context"
	"net/http"
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
		"no cluster in incoming context gives an ErrNoClusterValidationLabel error": {
			incomingContext: createContext(false, nil),
			expectedError:   ErrNoClusterValidationLabel,
		},
		"empty cluster in incoming context gives an ErrNoClusterValidationLabel error": {
			incomingContext: createContext(true, []string{""}),
			expectedError:   ErrNoClusterValidationLabel,
		},
		"single cluster in incoming context returns that cluster and no errors": {
			incomingContext: createContext(true, []string{"my-cluster"}),
			expectedError:   nil,
			expectedValue:   "my-cluster",
		},
		"more clusters in incoming context give an errDifferentClusterValidationLabels error": {
			incomingContext: createContext(true, []string{"cluster-1", "cluster-2"}),
			expectedError:   errDifferentClusterValidationLabels([]string{"cluster-1", "cluster-2"}),
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

func TestPutClusterIntoHeader(t *testing.T) {
	t.Run("no header is added to a nil request", func(t *testing.T) {
		var req *http.Request
		PutClusterIntoHeader(req, "cluster")
		require.Nil(t, req)
	})
	t.Run("ClusterValidationLabelHeader header is added to a non-nil request", func(t *testing.T) {
		req, err := http.NewRequest("GET", "http://localhost:8080/Test/Me", nil)
		require.NoError(t, err)
		require.NotNil(t, req)
		PutClusterIntoHeader(req, "cluster")
		require.Equal(t, "cluster", req.Header.Get(ClusterValidationLabelHeader))
	})
}

func TestGetClusterFromRequest(t *testing.T) {
	testCases := map[string]struct {
		request       *http.Request
		expectedValue string
		expectedError error
	}{
		"no cluster in request header gives an ErrNoClusterValidationLabelInHeader error": {
			request:       createRequest(false, nil),
			expectedError: ErrNoClusterValidationLabelInHeader,
		},
		"empty cluster in request header gives an ErrNoClusterValidationLabelInHeader error": {
			request:       createRequest(true, []string{""}),
			expectedError: ErrNoClusterValidationLabelInHeader,
		},
		"single cluster in request header returns that cluster and no error": {
			request:       createRequest(true, []string{"my-cluster"}),
			expectedError: nil,
			expectedValue: "my-cluster",
		},
		"more clusters in request header give an errDifferentClusterValidationLabelsInHeader error": {
			request:       createRequest(true, []string{"cluster-1", "cluster-2"}),
			expectedError: errDifferentClusterValidationLabelsInHeader([]string{"cluster-1", "cluster-2"}),
			expectedValue: "",
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			value, err := GetClusterFromRequest(testCase.request)
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
	values, ok := md[MetadataClusterValidationLabelKey]
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
		MetadataClusterValidationLabelKey: clusters,
	}
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestIsClusterAllowed(t *testing.T) {
	testCases := map[string]struct {
		cluster         string
		allowedClusters []string
		expectedAllowed bool
	}{
		"cluster found in single allowed cluster": {
			cluster:         "cluster-a",
			allowedClusters: []string{"cluster-a"},
			expectedAllowed: true,
		},
		"cluster found in multiple allowed clusters": {
			cluster:         "cluster-b",
			allowedClusters: []string{"cluster-a", "cluster-b", "cluster-c"},
			expectedAllowed: true,
		},
		"cluster not found in allowed clusters": {
			cluster:         "cluster-d",
			allowedClusters: []string{"cluster-a", "cluster-b", "cluster-c"},
			expectedAllowed: false,
		},
		"empty cluster with empty allowed clusters": {
			cluster:         "",
			allowedClusters: []string{},
			expectedAllowed: false,
		},
		"empty cluster with non-empty allowed clusters": {
			cluster:         "",
			allowedClusters: []string{"cluster-a", "cluster-b"},
			expectedAllowed: false,
		},
		"non-empty cluster with empty allowed clusters": {
			cluster:         "cluster-a",
			allowedClusters: []string{},
			expectedAllowed: false,
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			allowed := IsClusterAllowed(testCase.cluster, testCase.allowedClusters)
			require.Equal(t, testCase.expectedAllowed, allowed)
		})
	}
}

func createRequest(containsCluster bool, clusters []string) *http.Request {
	req := &http.Request{
		Header: make(http.Header),
	}
	if containsCluster {
		req.Header[ClusterValidationLabelHeader] = clusters
	}
	return req
}
