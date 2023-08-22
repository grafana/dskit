package metrics

import (
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestMatchesLabels(t *testing.T) {
	m := dto.Metric{Label: makeLabels(
		"label-1", "value-1",
		"label-2", "value-2",
	)}

	testCases := map[string]struct {
		labels        []string
		expectedMatch bool
	}{
		"no labels": {
			expectedMatch: true,
		},
		"one matching label": {
			labels:        []string{"label-1", "value-1"},
			expectedMatch: true,
		},
		"all matching labels": {
			labels: []string{
				"label-1", "value-1",
				"label-2", "value-2",
			},
			expectedMatch: true,
		},
		"different value for label in metric": {
			labels:        []string{"label-1", "value-2"},
			expectedMatch: false,
		},
		"label not on metric": {
			labels:        []string{"label-3", "value-2"},
			expectedMatch: false,
		},
		"one value matches, one does not": {
			labels: []string{
				"label-1", "value-1",
				"label-2", "value-3",
			},
			expectedMatch: false,
		},
		"one value matches, one label not on metric": {
			labels: []string{
				"label-1", "value-1",
				"label-3", "value-2",
			},
			expectedMatch: false,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := MatchesLabels(&m, testCase.labels...)
			require.Equal(t, testCase.expectedMatch, actual)
		})
	}
}

func TestFindMetricsInFamilyMatchingLabels(t *testing.T) {
	m1 := &dto.Metric{
		Label: makeLabels(
			"unique-label", "metric-1",
			"shared-label", "shared-value",
		),
	}
	m2 := &dto.Metric{
		Label: makeLabels(
			"unique-label", "metric-2",
			"shared-label", "shared-value",
		),
	}
	mf := &dto.MetricFamily{
		Metric: []*dto.Metric{m1, m2},
	}

	require.Empty(t, FindMetricsInFamilyMatchingLabels(mf, "another-label", "some-value"))
	require.Empty(t, FindMetricsInFamilyMatchingLabels(mf, "unique-label", "another-value"))
	require.ElementsMatch(t, FindMetricsInFamilyMatchingLabels(mf, "unique-label", "metric-1"), []*dto.Metric{m1})
	require.ElementsMatch(t, FindMetricsInFamilyMatchingLabels(mf, "shared-label", "shared-value"), []*dto.Metric{m1, m2})
}

func TestFindHistogramWithNameAndLabels(t *testing.T) {
	metricName := "the_metric"

	histogramMetric := &dto.Metric{
		Label: makeLabels(
			"unique-label", "histogram-metric",
			"shared-label", "shared-value",
		),
		Histogram: &dto.Histogram{},
	}

	nonHistogramMetric := &dto.Metric{
		Label: makeLabels(
			"unique-label", "non-histogram-metric",
			"shared-label", "shared-value",
		),
	}

	mf := &dto.MetricFamily{
		Name:   &metricName,
		Metric: []*dto.Metric{histogramMetric, nonHistogramMetric},
	}

	mfm := MetricFamilyMap{
		metricName: mf,
	}

	testCases := map[string]struct {
		metricName        string
		labels            []string
		expectedHistogram *dto.Histogram
		expectedError     string
	}{
		"no matching metric family": {
			metricName:    "another_metric",
			expectedError: "no metric with name 'another_metric' found",
		},
		"matching metric family, no matching metrics": {
			metricName:    metricName,
			labels:        []string{"unique-label", "another-value"},
			expectedError: "wanted exactly one matching metric, but found 0",
		},
		"matching metric family, multiple matching metrics": {
			metricName:    metricName,
			labels:        []string{"shared-label", "shared-value"},
			expectedError: "wanted exactly one matching metric, but found 2",
		},
		"matching metric family, one matching metric, metric is not a histogram": {
			metricName:    metricName,
			labels:        []string{"unique-label", "non-histogram-metric"},
			expectedError: "found a single matching metric, but it is not a histogram",
		},
		"matching metric family, one matching metric, metric is a histogram": {
			metricName:        metricName,
			labels:            []string{"unique-label", "histogram-metric"},
			expectedHistogram: histogramMetric.Histogram,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			actualHistogram, err := FindHistogramWithNameAndLabels(mfm, testCase.metricName, testCase.labels...)

			if testCase.expectedError == "" {
				require.NoError(t, err)
				require.Equal(t, testCase.expectedHistogram, actualHistogram)
			} else {
				require.EqualError(t, err, testCase.expectedError)
				require.Nil(t, actualHistogram)
			}
		})
	}
}
