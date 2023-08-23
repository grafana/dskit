package metrics

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSum(t *testing.T) {
	require.Equal(t, float64(0), sum(nil, counterValue))
	require.Equal(t, float64(0), sum(&dto.MetricFamily{Metric: nil}, counterValue))
	require.Equal(t, float64(0), sum(&dto.MetricFamily{Metric: []*dto.Metric{{Counter: &dto.Counter{}}}}, counterValue))
	require.Equal(t, 12345.6789, sum(&dto.MetricFamily{Metric: []*dto.Metric{{Counter: &dto.Counter{Value: proto.Float64(12345.6789)}}}}, counterValue))
	require.Equal(t, 20235.80235, sum(&dto.MetricFamily{Metric: []*dto.Metric{
		{Counter: &dto.Counter{Value: proto.Float64(12345.6789)}},
		{Counter: &dto.Counter{Value: proto.Float64(7890.12345)}},
	}}, counterValue))
	// using 'counterValue' as function only sums counters
	require.Equal(t, float64(0), sum(&dto.MetricFamily{Metric: []*dto.Metric{
		{Gauge: &dto.Gauge{Value: proto.Float64(12345.6789)}},
		{Gauge: &dto.Gauge{Value: proto.Float64(7890.12345)}},
	}}, counterValue))
}

func checkFloats(t *testing.T, exp, val float64) {
	if math.IsNaN(val) {
		if !math.IsNaN(exp) {
			require.Fail(t, "expected %s got NaN", exp)
		}
	} else if math.IsNaN(exp) {
		require.Fail(t, "expected NaN, got %s", val)
	} else {
		require.Equal(t, exp, val)
	}
}

func TestMinGauges(t *testing.T) {
	type testcase struct {
		exp float64
		mf  MetricFamilyMap
	}

	for _, tc := range []testcase{
		{exp: math.NaN(), mf: MetricFamilyMap{}},
		{exp: math.NaN(), mf: MetricFamilyMap{"metric": nil}},
		{exp: math.NaN(), mf: MetricFamilyMap{"metric": &dto.MetricFamily{Metric: nil}}},
		{exp: math.NaN(), mf: MetricFamilyMap{"metric": &dto.MetricFamily{Metric: []*dto.Metric{{Gauge: &dto.Gauge{}}}}}},
		{exp: 12345.6789, mf: MetricFamilyMap{"metric": &dto.MetricFamily{Metric: []*dto.Metric{{Gauge: &dto.Gauge{Value: proto.Float64(12345.6789)}}}}}},
		{exp: 1234.56789, mf: MetricFamilyMap{"metric": &dto.MetricFamily{Metric: []*dto.Metric{
			{Gauge: &dto.Gauge{Value: proto.Float64(1234.56789)}},
			{Gauge: &dto.Gauge{Value: proto.Float64(7890.12345)}},
		}}}},
	} {
		checkFloats(t, tc.mf.MinGauges("metric"), tc.exp)
	}
}

func TestMaxGauges(t *testing.T) {
	type testcase struct {
		exp float64
		mf  MetricFamilyMap
	}

	for _, tc := range []testcase{
		{exp: math.NaN(), mf: MetricFamilyMap{}},
		{exp: math.NaN(), mf: MetricFamilyMap{"metric": nil}},
		{exp: math.NaN(), mf: MetricFamilyMap{"metric": &dto.MetricFamily{Metric: nil}}},
		{exp: math.NaN(), mf: MetricFamilyMap{"metric": &dto.MetricFamily{Metric: []*dto.Metric{{Gauge: &dto.Gauge{}}}}}},
		{exp: 12345.6789, mf: MetricFamilyMap{"metric": &dto.MetricFamily{Metric: []*dto.Metric{{Gauge: &dto.Gauge{Value: proto.Float64(12345.6789)}}}}}},
		{exp: 7890.12345, mf: MetricFamilyMap{"metric": &dto.MetricFamily{Metric: []*dto.Metric{
			{Gauge: &dto.Gauge{Value: proto.Float64(1234.56789)}},
			{Gauge: &dto.Gauge{Value: proto.Float64(7890.12345)}},
		}}}},
	} {
		checkFloats(t, tc.mf.MaxGauges("metric"), tc.exp)
	}
}

func TestCounterValue(t *testing.T) {
	require.Equal(t, float64(0), counterValue(nil))
	require.Equal(t, float64(0), counterValue(&dto.Metric{}))
	require.Equal(t, float64(0), counterValue(&dto.Metric{Counter: &dto.Counter{}}))
	require.Equal(t, 543857.12837, counterValue(&dto.Metric{Counter: &dto.Counter{Value: proto.Float64(543857.12837)}}))
}

func TestGaugeValueNan(t *testing.T) {
	require.True(t, math.IsNaN(gaugeValueOrNaN(nil)))
	require.True(t, math.IsNaN(gaugeValueOrNaN(&dto.Metric{})))
	require.True(t, math.IsNaN(gaugeValueOrNaN(&dto.Metric{Gauge: &dto.Gauge{}})))
	require.Equal(t, 543857.12837, gaugeValueOrNaN(&dto.Metric{Gauge: &dto.Gauge{Value: proto.Float64(543857.12837)}}))
}

func TestGetMetricsWithLabelNames(t *testing.T) {
	labels := []string{"a", "b"}

	require.Equal(t, map[string]metricsWithLabels{}, getMetricsWithLabelNames(nil, labels))
	require.Equal(t, map[string]metricsWithLabels{}, getMetricsWithLabelNames(&dto.MetricFamily{}, labels))

	m1 := &dto.Metric{Label: makeLabels("a", "5"), Counter: &dto.Counter{Value: proto.Float64(1)}}
	m2 := &dto.Metric{Label: makeLabels("a", "10", "b", "20"), Counter: &dto.Counter{Value: proto.Float64(1.5)}}
	m3 := &dto.Metric{Label: makeLabels("a", "10", "b", "20", "c", "1"), Counter: &dto.Counter{Value: proto.Float64(2)}}
	m4 := &dto.Metric{Label: makeLabels("a", "10", "b", "20", "c", "2"), Counter: &dto.Counter{Value: proto.Float64(3)}}
	m5 := &dto.Metric{Label: makeLabels("a", "11", "b", "21"), Counter: &dto.Counter{Value: proto.Float64(4)}}
	m6 := &dto.Metric{Label: makeLabels("ignored", "123", "a", "12", "b", "22", "c", "30"), Counter: &dto.Counter{Value: proto.Float64(4)}}

	out := getMetricsWithLabelNames(&dto.MetricFamily{Metric: []*dto.Metric{m1, m2, m3, m4, m5, m6}}, labels)

	// m1 is not returned at all, as it doesn't have both required labels.
	require.Equal(t, map[string]metricsWithLabels{
		getLabelsString([]string{"10", "20"}): {
			labelValues: []string{"10", "20"},
			metrics:     []*dto.Metric{m2, m3, m4}},
		getLabelsString([]string{"11", "21"}): {
			labelValues: []string{"11", "21"},
			metrics:     []*dto.Metric{m5}},
		getLabelsString([]string{"12", "22"}): {
			labelValues: []string{"12", "22"},
			metrics:     []*dto.Metric{m6}},
	}, out)

	// no labels -- returns all metrics in single key. this isn't very efficient, and there are other functions
	// (without labels) to handle this better, but it still works.
	out2 := getMetricsWithLabelNames(&dto.MetricFamily{Metric: []*dto.Metric{m1, m2, m3, m4, m5, m6}}, nil)
	require.Equal(t, map[string]metricsWithLabels{
		getLabelsString(nil): {
			labelValues: []string{},
			metrics:     []*dto.Metric{m1, m2, m3, m4, m5, m6}},
	}, out2)
}

func BenchmarkGetMetricsWithLabelNames(b *testing.B) {
	const (
		numMetrics         = 1000
		numLabelsPerMetric = 10
	)

	// Generate metrics and add them to a metric family.
	mf := &dto.MetricFamily{Metric: make([]*dto.Metric, 0, numMetrics)}
	for i := 0; i < numMetrics; i++ {
		labels := []*dto.LabelPair{{
			Name:  proto.String("unique"),
			Value: proto.String(strconv.Itoa(i)),
		}}

		for l := 1; l < numLabelsPerMetric; l++ {
			labels = append(labels, &dto.LabelPair{
				Name:  proto.String(fmt.Sprintf("label_%d", l)),
				Value: proto.String(fmt.Sprintf("value_%d", l)),
			})
		}

		mf.Metric = append(mf.Metric, &dto.Metric{
			Label:   labels,
			Counter: &dto.Counter{Value: proto.Float64(1.5)},
		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		out := getMetricsWithLabelNames(mf, []string{"label_1", "label_2", "label_3"})

		if expected := 1; len(out) != expected {
			b.Fatalf("unexpected number of output groups: expected = %d got = %d", expected, len(out))
		}
	}
}

// TestSendSumOfGaugesPerTenantWithLabels tests to ensure multiple metrics for the same tenant with a matching label are
// summed correctly.
func TestSendSumOfGaugesPerTenantWithLabels(t *testing.T) {
	user1Reg := prometheus.NewRegistry()
	user2Reg := prometheus.NewRegistry()
	user1Metric := promauto.With(user1Reg).NewGaugeVec(prometheus.GaugeOpts{Name: "test_metric"}, []string{"label_one", "label_two"})
	user2Metric := promauto.With(user2Reg).NewGaugeVec(prometheus.GaugeOpts{Name: "test_metric"}, []string{"label_one", "label_two"})
	user1Metric.WithLabelValues("a", "b").Set(100)
	user1Metric.WithLabelValues("a", "c").Set(80)
	user2Metric.WithLabelValues("a", "b").Set(60)
	user2Metric.WithLabelValues("a", "c").Set(40)

	regs := NewTenantRegistries(log.NewNopLogger())
	regs.AddTenantRegistry("user-1", user1Reg)
	regs.AddTenantRegistry("user-2", user2Reg)
	mf := regs.BuildMetricFamiliesPerTenant()

	{
		desc := prometheus.NewDesc("test_metric", "", []string{"user", "label_one"}, nil)
		actual := collectMetrics(t, func(out chan prometheus.Metric) {
			mf.SendSumOfGaugesPerTenantWithLabels(out, desc, "test_metric", "label_one")
		})
		expected := []*dto.Metric{
			{Label: makeLabels("label_one", "a", "user", "user-1"), Gauge: &dto.Gauge{Value: proto.Float64(180)}},
			{Label: makeLabels("label_one", "a", "user", "user-2"), Gauge: &dto.Gauge{Value: proto.Float64(100)}},
		}
		require.ElementsMatch(t, expected, actual)
	}

	{
		desc := prometheus.NewDesc("test_metric", "", []string{"user", "label_two"}, nil)
		actual := collectMetrics(t, func(out chan prometheus.Metric) {
			mf.SendSumOfGaugesPerTenantWithLabels(out, desc, "test_metric", "label_two")
		})
		expected := []*dto.Metric{
			{Label: makeLabels("label_two", "b", "user", "user-1"), Gauge: &dto.Gauge{Value: proto.Float64(100)}},
			{Label: makeLabels("label_two", "c", "user", "user-1"), Gauge: &dto.Gauge{Value: proto.Float64(80)}},
			{Label: makeLabels("label_two", "b", "user", "user-2"), Gauge: &dto.Gauge{Value: proto.Float64(60)}},
			{Label: makeLabels("label_two", "c", "user", "user-2"), Gauge: &dto.Gauge{Value: proto.Float64(40)}},
		}
		require.ElementsMatch(t, expected, actual)
	}

	{
		desc := prometheus.NewDesc("test_metric", "", []string{"user", "label_one", "label_two"}, nil)
		actual := collectMetrics(t, func(out chan prometheus.Metric) {
			mf.SendSumOfGaugesPerTenantWithLabels(out, desc, "test_metric", "label_one", "label_two")
		})
		expected := []*dto.Metric{
			{Label: makeLabels("label_one", "a", "label_two", "b", "user", "user-1"), Gauge: &dto.Gauge{Value: proto.Float64(100)}},
			{Label: makeLabels("label_one", "a", "label_two", "c", "user", "user-1"), Gauge: &dto.Gauge{Value: proto.Float64(80)}},
			{Label: makeLabels("label_one", "a", "label_two", "b", "user", "user-2"), Gauge: &dto.Gauge{Value: proto.Float64(60)}},
			{Label: makeLabels("label_one", "a", "label_two", "c", "user", "user-2"), Gauge: &dto.Gauge{Value: proto.Float64(40)}},
		}
		require.ElementsMatch(t, expected, actual)
	}
}

func checkSingleGauge(t *testing.T, actual []*dto.Metric, val float64) {
	require.Len(t, actual, 1)
	require.Nil(t, actual[0].Label)
	g := actual[0].GetGauge()
	require.NotNil(t, g)
	require.Equal(t, val, g.GetValue())
}

func TestSendMinMaxOfGauges(t *testing.T) {
	const (
		userLabel = "user"
		user1     = "user-1"
		user2     = "user-2"
		user3     = "user-3"
	)

	user1Reg := prometheus.NewRegistry()
	user2Reg := prometheus.NewRegistry()
	user3Reg := prometheus.NewRegistry()
	desc := prometheus.NewDesc("test_metric", "", nil, nil)
	descUser := prometheus.NewDesc("per_tenant", "", []string{userLabel}, nil)
	regs := NewTenantRegistries(log.NewNopLogger())

	regs.AddTenantRegistry(user1, user1Reg)
	regs.AddTenantRegistry(user2, user2Reg)
	regs.AddTenantRegistry(user3, user3Reg)

	verifySendMinOfGauges := func(t *testing.T, val float64) {
		mf := regs.BuildMetricFamiliesPerTenant()
		actual := collectMetrics(t, func(out chan prometheus.Metric) {
			mf.SendMinOfGauges(out, desc, "test_metric")
		})
		checkSingleGauge(t, actual, val)
	}

	verifySendMaxOfGauges := func(t *testing.T, val float64) {
		mf := regs.BuildMetricFamiliesPerTenant()
		actual := collectMetrics(t, func(out chan prometheus.Metric) {
			mf.SendMaxOfGauges(out, desc, "test_metric")
		})
		checkSingleGauge(t, actual, val)
	}

	verifySendMaxOfGaugesPerTenant := func(t *testing.T, perTenantValues map[string]float64) {
		mf := regs.BuildMetricFamiliesPerTenant()
		collected := collectMetrics(t, func(out chan prometheus.Metric) {
			mf.SendMaxOfGaugesPerTenant(out, descUser, "test_metric")
		})

		require.Len(t, collected, len(perTenantValues))
		for _, a := range collected {
			require.Len(t, a.GetLabel(), 1)
			require.Equal(t, userLabel, a.GetLabel()[0].GetName())
			tenant := a.GetLabel()[0].GetValue()
			val, ok := perTenantValues[tenant]
			require.True(t, ok, "tenant not found in the map: %s", tenant)
			g := a.GetGauge()
			require.NotNil(t, g)
			require.Equal(t, val, g.GetValue())

			delete(perTenantValues, tenant)
		}

		require.Len(t, perTenantValues, 0)
	}

	// No matching metric.
	verifySendMinOfGauges(t, 0)
	verifySendMaxOfGauges(t, 0)
	verifySendMaxOfGaugesPerTenant(t, map[string]float64{})

	// Register a metric for one user.
	user1Metric := promauto.With(user1Reg).NewGauge(prometheus.GaugeOpts{Name: "test_metric"})
	user1Metric.Set(100)

	verifySendMinOfGauges(t, 100)
	verifySendMaxOfGauges(t, 100)
	verifySendMaxOfGaugesPerTenant(t, map[string]float64{user1: 100})

	// Register metric for another user.
	user2Metric := promauto.With(user2Reg).NewGauge(prometheus.GaugeOpts{Name: "test_metric"})
	user2Metric.Set(80)

	verifySendMinOfGauges(t, 80)
	verifySendMaxOfGauges(t, 100)
	verifySendMaxOfGaugesPerTenant(t, map[string]float64{user1: 100, user2: 80})

	// Register metric for third user, with negative value.
	user3Metric := promauto.With(user3Reg).NewGauge(prometheus.GaugeOpts{Name: "test_metric"})
	user3Metric.Set(-1000)

	verifySendMinOfGauges(t, -1000)
	verifySendMaxOfGauges(t, 100)
	verifySendMaxOfGaugesPerTenant(t, map[string]float64{user1: 100, user2: 80, user3: -1000})

	// Remove gauge from first user.
	user1Reg.Unregister(user1Metric)
	verifySendMinOfGauges(t, -1000)
	verifySendMaxOfGauges(t, 80)
	verifySendMaxOfGaugesPerTenant(t, map[string]float64{user2: 80, user3: -1000})

	// Remove gauge from second user.
	user2Reg.Unregister(user2Metric)
	verifySendMinOfGauges(t, -1000)
	verifySendMaxOfGauges(t, -1000)
	verifySendMaxOfGaugesPerTenant(t, map[string]float64{user3: -1000})

	// Remove gauge from third user.
	user3Reg.Unregister(user3Metric)
	verifySendMinOfGauges(t, 0)
	verifySendMaxOfGauges(t, 0)
	verifySendMaxOfGaugesPerTenant(t, map[string]float64{})
}

func TestSendSumOfHistogramsWithLabels(t *testing.T) {
	buckets := []float64{1, 2, 3}
	user1Reg := prometheus.NewRegistry()
	user2Reg := prometheus.NewRegistry()
	user1Metric := promauto.With(user1Reg).NewHistogramVec(prometheus.HistogramOpts{Name: "test_metric", Buckets: buckets}, []string{"label_one", "label_two"})
	user2Metric := promauto.With(user2Reg).NewHistogramVec(prometheus.HistogramOpts{Name: "test_metric", Buckets: buckets}, []string{"label_one", "label_two"})
	user1Metric.WithLabelValues("a", "b").Observe(1)
	user1Metric.WithLabelValues("a", "c").Observe(2)
	user2Metric.WithLabelValues("a", "b").Observe(3)
	user2Metric.WithLabelValues("a", "c").Observe(4)

	regs := NewTenantRegistries(log.NewNopLogger())
	regs.AddTenantRegistry("user-1", user1Reg)
	regs.AddTenantRegistry("user-2", user2Reg)
	mf := regs.BuildMetricFamiliesPerTenant()

	{
		desc := prometheus.NewDesc("test_metric", "", []string{"label_one"}, nil)
		actual := collectMetrics(t, func(out chan prometheus.Metric) {
			mf.SendSumOfHistogramsWithLabels(out, desc, "test_metric", "label_one")
		})
		expected := []*dto.Metric{
			{Label: makeLabels("label_one", "a"), Histogram: &dto.Histogram{SampleCount: uint64p(4), SampleSum: float64p(10), Bucket: []*dto.Bucket{
				{UpperBound: float64p(1), CumulativeCount: uint64p(1)},
				{UpperBound: float64p(2), CumulativeCount: uint64p(2)},
				{UpperBound: float64p(3), CumulativeCount: uint64p(3)},
			}}},
		}
		require.ElementsMatch(t, expected, actual)
	}

	{
		desc := prometheus.NewDesc("test_metric", "", []string{"label_two"}, nil)
		actual := collectMetrics(t, func(out chan prometheus.Metric) {
			mf.SendSumOfHistogramsWithLabels(out, desc, "test_metric", "label_two")
		})
		expected := []*dto.Metric{
			{Label: makeLabels("label_two", "b"), Histogram: &dto.Histogram{SampleCount: uint64p(2), SampleSum: float64p(4), Bucket: []*dto.Bucket{
				{UpperBound: float64p(1), CumulativeCount: uint64p(1)},
				{UpperBound: float64p(2), CumulativeCount: uint64p(1)},
				{UpperBound: float64p(3), CumulativeCount: uint64p(2)},
			}}},
			{Label: makeLabels("label_two", "c"), Histogram: &dto.Histogram{SampleCount: uint64p(2), SampleSum: float64p(6), Bucket: []*dto.Bucket{
				{UpperBound: float64p(1), CumulativeCount: uint64p(0)},
				{UpperBound: float64p(2), CumulativeCount: uint64p(1)},
				{UpperBound: float64p(3), CumulativeCount: uint64p(1)},
			}}},
		}
		require.ElementsMatch(t, expected, actual)
	}

	{
		desc := prometheus.NewDesc("test_metric", "", []string{"label_one", "label_two"}, nil)
		actual := collectMetrics(t, func(out chan prometheus.Metric) {
			mf.SendSumOfHistogramsWithLabels(out, desc, "test_metric", "label_one", "label_two")
		})
		expected := []*dto.Metric{
			{Label: makeLabels("label_one", "a", "label_two", "b"), Histogram: &dto.Histogram{SampleCount: uint64p(2), SampleSum: float64p(4), Bucket: []*dto.Bucket{
				{UpperBound: float64p(1), CumulativeCount: uint64p(1)},
				{UpperBound: float64p(2), CumulativeCount: uint64p(1)},
				{UpperBound: float64p(3), CumulativeCount: uint64p(2)},
			}}},
			{Label: makeLabels("label_one", "a", "label_two", "c"), Histogram: &dto.Histogram{SampleCount: uint64p(2), SampleSum: float64p(6), Bucket: []*dto.Bucket{
				{UpperBound: float64p(1), CumulativeCount: uint64p(0)},
				{UpperBound: float64p(2), CumulativeCount: uint64p(1)},
				{UpperBound: float64p(3), CumulativeCount: uint64p(1)},
			}}},
		}
		require.ElementsMatch(t, expected, actual)
	}
}

// TestSendSumOfCountersPerTenant_WithLabels tests to ensure multiple metrics for the same user with a matching label are
// summed correctly
func TestSendSumOfCountersPerTenant_WithLabels(t *testing.T) {
	user1Reg := prometheus.NewRegistry()
	user2Reg := prometheus.NewRegistry()
	user3Reg := prometheus.NewRegistry()
	user1Metric := promauto.With(user1Reg).NewCounterVec(prometheus.CounterOpts{Name: "test_metric"}, []string{"label_one", "label_two"})
	user2Metric := promauto.With(user2Reg).NewCounterVec(prometheus.CounterOpts{Name: "test_metric"}, []string{"label_one", "label_two"})
	user3Metric := promauto.With(user3Reg).NewCounterVec(prometheus.CounterOpts{Name: "test_metric"}, []string{"label_one", "label_two"})
	user1Metric.WithLabelValues("a", "b").Add(100)
	user1Metric.WithLabelValues("a", "c").Add(80)
	user2Metric.WithLabelValues("a", "b").Add(60)
	user2Metric.WithLabelValues("a", "c").Add(0)
	user3Metric.WithLabelValues("a", "b").Add(0)
	user3Metric.WithLabelValues("a", "c").Add(0)

	regs := NewTenantRegistries(log.NewNopLogger())
	regs.AddTenantRegistry("user-1", user1Reg)
	regs.AddTenantRegistry("user-2", user2Reg)
	regs.AddTenantRegistry("user-3", user3Reg)
	mf := regs.BuildMetricFamiliesPerTenant()

	t.Run("group metrics by user and label_one", func(t *testing.T) {
		desc := prometheus.NewDesc("test_metric", "", []string{"user", "label_one"}, nil)
		actual := collectMetrics(t, func(out chan prometheus.Metric) {
			mf.SendSumOfCountersPerTenant(out, desc, "test_metric", WithLabels("label_one"))
		})
		expected := []*dto.Metric{
			{Label: makeLabels("label_one", "a", "user", "user-1"), Counter: &dto.Counter{Value: proto.Float64(180)}},
			{Label: makeLabels("label_one", "a", "user", "user-2"), Counter: &dto.Counter{Value: proto.Float64(60)}},
			{Label: makeLabels("label_one", "a", "user", "user-3"), Counter: &dto.Counter{Value: proto.Float64(0)}},
		}
		require.ElementsMatch(t, expected, actual)
	})

	t.Run("group metrics by user and label_one, and skip zero value metrics", func(t *testing.T) {
		desc := prometheus.NewDesc("test_metric", "", []string{"user", "label_one"}, nil)
		actual := collectMetrics(t, func(out chan prometheus.Metric) {
			mf.SendSumOfCountersPerTenant(out, desc, "test_metric", WithLabels("label_one"), WithSkipZeroValueMetrics)
		})
		expected := []*dto.Metric{
			{Label: makeLabels("label_one", "a", "user", "user-1"), Counter: &dto.Counter{Value: proto.Float64(180)}},
			{Label: makeLabels("label_one", "a", "user", "user-2"), Counter: &dto.Counter{Value: proto.Float64(60)}},
		}
		require.ElementsMatch(t, expected, actual)
	})

	t.Run("group metrics by user and label_two", func(t *testing.T) {
		desc := prometheus.NewDesc("test_metric", "", []string{"user", "label_two"}, nil)
		actual := collectMetrics(t, func(out chan prometheus.Metric) {
			mf.SendSumOfCountersPerTenant(out, desc, "test_metric", WithLabels("label_two"))
		})
		expected := []*dto.Metric{
			{Label: makeLabels("label_two", "b", "user", "user-1"), Counter: &dto.Counter{Value: proto.Float64(100)}},
			{Label: makeLabels("label_two", "c", "user", "user-1"), Counter: &dto.Counter{Value: proto.Float64(80)}},
			{Label: makeLabels("label_two", "b", "user", "user-2"), Counter: &dto.Counter{Value: proto.Float64(60)}},
			{Label: makeLabels("label_two", "c", "user", "user-2"), Counter: &dto.Counter{Value: proto.Float64(0)}},
			{Label: makeLabels("label_two", "b", "user", "user-3"), Counter: &dto.Counter{Value: proto.Float64(0)}},
			{Label: makeLabels("label_two", "c", "user", "user-3"), Counter: &dto.Counter{Value: proto.Float64(0)}},
		}
		require.ElementsMatch(t, expected, actual)
	})

	t.Run("group metrics by user and label_two, and skip zero value metrics", func(t *testing.T) {
		desc := prometheus.NewDesc("test_metric", "", []string{"user", "label_two"}, nil)
		actual := collectMetrics(t, func(out chan prometheus.Metric) {
			mf.SendSumOfCountersPerTenant(out, desc, "test_metric", WithLabels("label_two"), WithSkipZeroValueMetrics)
		})
		expected := []*dto.Metric{
			{Label: makeLabels("label_two", "b", "user", "user-1"), Counter: &dto.Counter{Value: proto.Float64(100)}},
			{Label: makeLabels("label_two", "c", "user", "user-1"), Counter: &dto.Counter{Value: proto.Float64(80)}},
			{Label: makeLabels("label_two", "b", "user", "user-2"), Counter: &dto.Counter{Value: proto.Float64(60)}},
		}
		require.ElementsMatch(t, expected, actual)
	})

	t.Run("group metrics by user, label_one and label_two", func(t *testing.T) {
		desc := prometheus.NewDesc("test_metric", "", []string{"user", "label_one", "label_two"}, nil)
		actual := collectMetrics(t, func(out chan prometheus.Metric) {
			mf.SendSumOfCountersPerTenant(out, desc, "test_metric", WithLabels("label_one", "label_two"))
		})
		expected := []*dto.Metric{
			{Label: makeLabels("label_one", "a", "label_two", "b", "user", "user-1"), Counter: &dto.Counter{Value: proto.Float64(100)}},
			{Label: makeLabels("label_one", "a", "label_two", "c", "user", "user-1"), Counter: &dto.Counter{Value: proto.Float64(80)}},
			{Label: makeLabels("label_one", "a", "label_two", "b", "user", "user-2"), Counter: &dto.Counter{Value: proto.Float64(60)}},
			{Label: makeLabels("label_one", "a", "label_two", "c", "user", "user-2"), Counter: &dto.Counter{Value: proto.Float64(0)}},
			{Label: makeLabels("label_one", "a", "label_two", "b", "user", "user-3"), Counter: &dto.Counter{Value: proto.Float64(0)}},
			{Label: makeLabels("label_one", "a", "label_two", "c", "user", "user-3"), Counter: &dto.Counter{Value: proto.Float64(0)}},
		}
		require.ElementsMatch(t, expected, actual)
	})

	t.Run("group metrics by user, label_one and label_two, and skip zero value metrics", func(t *testing.T) {
		desc := prometheus.NewDesc("test_metric", "", []string{"user", "label_one", "label_two"}, nil)
		actual := collectMetrics(t, func(out chan prometheus.Metric) {
			mf.SendSumOfCountersPerTenant(out, desc, "test_metric", WithLabels("label_one", "label_two"), WithSkipZeroValueMetrics)
		})
		expected := []*dto.Metric{
			{Label: makeLabels("label_one", "a", "label_two", "b", "user", "user-1"), Counter: &dto.Counter{Value: proto.Float64(100)}},
			{Label: makeLabels("label_one", "a", "label_two", "c", "user", "user-1"), Counter: &dto.Counter{Value: proto.Float64(80)}},
			{Label: makeLabels("label_one", "a", "label_two", "b", "user", "user-2"), Counter: &dto.Counter{Value: proto.Float64(60)}},
		}
		require.ElementsMatch(t, expected, actual)
	})
}

func TestSendSumOfSummariesPerTenant(t *testing.T) {
	objectives := map[float64]float64{0.25: 25, 0.5: 50, 0.75: 75}
	user1Reg := prometheus.NewRegistry()
	user2Reg := prometheus.NewRegistry()
	user1Metric := promauto.With(user1Reg).NewSummary(prometheus.SummaryOpts{Name: "test_metric", Objectives: objectives})
	user2Metric := promauto.With(user2Reg).NewSummary(prometheus.SummaryOpts{Name: "test_metric", Objectives: objectives})
	user1Metric.Observe(25)
	user1Metric.Observe(50)
	user1Metric.Observe(75)
	user2Metric.Observe(25)
	user2Metric.Observe(50)
	user2Metric.Observe(76)

	regs := NewTenantRegistries(log.NewNopLogger())
	regs.AddTenantRegistry("user-1", user1Reg)
	regs.AddTenantRegistry("user-2", user2Reg)
	mf := regs.BuildMetricFamiliesPerTenant()

	{
		desc := prometheus.NewDesc("test_metric", "", []string{"user"}, nil)
		actual := collectMetrics(t, func(out chan prometheus.Metric) {
			mf.SendSumOfSummariesPerTenant(out, desc, "test_metric")
		})
		expected := []*dto.Metric{
			{
				Label: makeLabels("user", "user-1"),
				Summary: &dto.Summary{
					SampleCount: uint64p(3),
					SampleSum:   float64p(150),
					Quantile: []*dto.Quantile{
						{
							Quantile: proto.Float64(.25),
							Value:    proto.Float64(25),
						},
						{
							Quantile: proto.Float64(.5),
							Value:    proto.Float64(50),
						},
						{
							Quantile: proto.Float64(.75),
							Value:    proto.Float64(75),
						},
					},
				},
			},
			{
				Label: makeLabels("user", "user-2"),
				Summary: &dto.Summary{
					SampleCount: uint64p(3),
					SampleSum:   float64p(151),
					Quantile: []*dto.Quantile{
						{
							Quantile: proto.Float64(.25),
							Value:    proto.Float64(25),
						},
						{
							Quantile: proto.Float64(.5),
							Value:    proto.Float64(50),
						},
						{
							Quantile: proto.Float64(.75),
							Value:    proto.Float64(76),
						},
					},
				},
			},
		}
		require.ElementsMatch(t, expected, actual)
	}
}

func TestFloat64PrecisionStability(t *testing.T) {
	const (
		numRuns       = 100
		numRegistries = 100
		cardinality   = 20
	)

	// Randomise the seed but log it in case we need to reproduce the test on failure.
	seed := time.Now().UnixNano()
	rand.Seed(seed)
	t.Log("random generator seed:", seed)

	// Generate a large number of registries with different metrics each.
	registries := NewTenantRegistries(log.NewNopLogger())
	for userID := 1; userID <= numRegistries; userID++ {
		reg := prometheus.NewRegistry()
		labelNames := []string{"label_one", "label_two"}

		g := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{Name: "test_gauge"}, labelNames)
		for i := 0; i < cardinality; i++ {
			g.WithLabelValues("a", strconv.Itoa(i)).Set(rand.Float64())
		}

		c := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{Name: "test_counter"}, labelNames)
		for i := 0; i < cardinality; i++ {
			c.WithLabelValues("a", strconv.Itoa(i)).Add(rand.Float64())
		}

		h := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{Name: "test_histogram", Buckets: []float64{0.1, 0.5, 1}}, labelNames)
		for i := 0; i < cardinality; i++ {
			h.WithLabelValues("a", strconv.Itoa(i)).Observe(rand.Float64())
		}

		s := promauto.With(reg).NewSummaryVec(prometheus.SummaryOpts{Name: "test_summary"}, labelNames)
		for i := 0; i < cardinality; i++ {
			s.WithLabelValues("a", strconv.Itoa(i)).Observe(rand.Float64())
		}

		registries.AddTenantRegistry(strconv.Itoa(userID), reg)
	}

	// Ensure multiple runs always return the same exact results.
	expected := map[string][]*dto.Metric{}

	for run := 0; run < numRuns; run++ {
		mf := registries.BuildMetricFamiliesPerTenant()

		gauge := collectMetrics(t, func(out chan prometheus.Metric) {
			mf.SendSumOfGauges(out, prometheus.NewDesc("test_gauge", "", nil, nil), "test_gauge")
		})
		gaugeWithLabels := collectMetrics(t, func(out chan prometheus.Metric) {
			mf.SendSumOfGaugesWithLabels(out, prometheus.NewDesc("test_gauge", "", []string{"label_one"}, nil), "test_gauge", "label_one")
		})

		counter := collectMetrics(t, func(out chan prometheus.Metric) {
			mf.SendSumOfCounters(out, prometheus.NewDesc("test_counter", "", nil, nil), "test_counter")
		})
		counterWithLabels := collectMetrics(t, func(out chan prometheus.Metric) {
			mf.SendSumOfCountersWithLabels(out, prometheus.NewDesc("test_counter", "", []string{"label_one"}, nil), "test_counter", "label_one")
		})

		histogram := collectMetrics(t, func(out chan prometheus.Metric) {
			mf.SendSumOfHistograms(out, prometheus.NewDesc("test_histogram", "", nil, nil), "test_histogram")
		})
		histogramWithLabels := collectMetrics(t, func(out chan prometheus.Metric) {
			mf.SendSumOfHistogramsWithLabels(out, prometheus.NewDesc("test_histogram", "", []string{"label_one"}, nil), "test_histogram", "label_one")
		})

		summary := collectMetrics(t, func(out chan prometheus.Metric) {
			mf.SendSumOfSummaries(out, prometheus.NewDesc("test_summary", "", nil, nil), "test_summary")
		})
		summaryWithLabels := collectMetrics(t, func(out chan prometheus.Metric) {
			mf.SendSumOfSummariesWithLabels(out, prometheus.NewDesc("test_summary", "", []string{"label_one"}, nil), "test_summary", "label_one")
		})

		// The first run we just store the expected value.
		if run == 0 {
			expected["gauge"] = gauge
			expected["gauge_with_labels"] = gaugeWithLabels
			expected["counter"] = counter
			expected["counter_with_labels"] = counterWithLabels
			expected["histogram"] = histogram
			expected["histogram_with_labels"] = histogramWithLabels
			expected["summary"] = summary
			expected["summary_with_labels"] = summaryWithLabels
			continue
		}

		// All subsequent runs we assert the actual metric with the expected one.
		require.Equal(t, expected["gauge"], gauge)
		require.Equal(t, expected["gauge_with_labels"], gaugeWithLabels)
		require.Equal(t, expected["counter"], counter)
		require.Equal(t, expected["counter_with_labels"], counterWithLabels)
		require.Equal(t, expected["histogram"], histogram)
		require.Equal(t, expected["histogram_with_labels"], histogramWithLabels)
		require.Equal(t, expected["summary"], summary)
		require.Equal(t, expected["summary_with_labels"], summaryWithLabels)
	}
}

// This test is a baseline for following tests, that remove or replace a registry.
// It shows output for metrics from setupTestMetrics before doing any modifications.
func TestTenantRegistries_RemoveBaseline(t *testing.T) {
	mainRegistry := prometheus.NewPedanticRegistry()
	mainRegistry.MustRegister(setupTestMetrics())

	require.NoError(t, testutil.GatherAndCompare(mainRegistry, bytes.NewBufferString(`
		# HELP counter help
		# TYPE counter counter
		counter 75

		# HELP counter_labels help
		# TYPE counter_labels counter
		counter_labels{label_one="a"} 75

		# HELP counter_user help
		# TYPE counter_user counter
		counter_user{user="1"} 5
		counter_user{user="2"} 10
		counter_user{user="3"} 15
		counter_user{user="4"} 20
		counter_user{user="5"} 25

		# HELP gauge help
		# TYPE gauge gauge
		gauge 75

		# HELP gauge_labels help
		# TYPE gauge_labels gauge
		gauge_labels{label_one="a"} 75

		# HELP gauge_user help
		# TYPE gauge_user gauge
		gauge_user{user="1"} 5
		gauge_user{user="2"} 10
		gauge_user{user="3"} 15
		gauge_user{user="4"} 20
		gauge_user{user="5"} 25

		# HELP histogram help
		# TYPE histogram histogram
		histogram_bucket{le="1"} 5
		histogram_bucket{le="3"} 15
		histogram_bucket{le="5"} 25
		histogram_bucket{le="+Inf"} 25
		histogram_sum 75
		histogram_count 25

		# HELP histogram_labels help
		# TYPE histogram_labels histogram
		histogram_labels_bucket{label_one="a",le="1"} 5
		histogram_labels_bucket{label_one="a",le="3"} 15
		histogram_labels_bucket{label_one="a",le="5"} 25
		histogram_labels_bucket{label_one="a",le="+Inf"} 25
		histogram_labels_sum{label_one="a"} 75
		histogram_labels_count{label_one="a"} 25

		# HELP summary help
		# TYPE summary summary
		summary_sum 75
		summary_count 25

		# HELP summary_labels help
		# TYPE summary_labels summary
		summary_labels_sum{label_one="a"} 75
		summary_labels_count{label_one="a"} 25

		# HELP summary_user help
		# TYPE summary_user summary
		summary_user_sum{user="1"} 5
		summary_user_count{user="1"} 5
		summary_user_sum{user="2"} 10
		summary_user_count{user="2"} 5
		summary_user_sum{user="3"} 15
		summary_user_count{user="3"} 5
		summary_user_sum{user="4"} 20
		summary_user_count{user="4"} 5
		summary_user_sum{user="5"} 25
		summary_user_count{user="5"} 5
	`)))
}

func TestTenantRegistries_RemoveTenantRegistry_SoftRemoval(t *testing.T) {
	tm := setupTestMetrics()

	mainRegistry := prometheus.NewPedanticRegistry()
	mainRegistry.MustRegister(tm)

	// Soft-remove single registry.
	tm.regs.RemoveTenantRegistry(strconv.Itoa(3), false)

	require.NoError(t, testutil.GatherAndCompare(mainRegistry, bytes.NewBufferString(`
			# HELP counter help
			# TYPE counter counter
	# No change in counter
			counter 75
	
			# HELP counter_labels help
			# TYPE counter_labels counter
	# No change in counter per label.
			counter_labels{label_one="a"} 75
	
			# HELP counter_user help
			# TYPE counter_user counter
	# User 3 is now missing.
			counter_user{user="1"} 5
			counter_user{user="2"} 10
			counter_user{user="4"} 20
			counter_user{user="5"} 25
	
			# HELP gauge help
			# TYPE gauge gauge
	# Drop in the gauge (value 3, counted 5 times)
			gauge 60
	
			# HELP gauge_labels help
			# TYPE gauge_labels gauge
	# Drop in the gauge (value 3, counted 5 times)
			gauge_labels{label_one="a"} 60
	
			# HELP gauge_user help
			# TYPE gauge_user gauge
	# User 3 is now missing.
			gauge_user{user="1"} 5
			gauge_user{user="2"} 10
			gauge_user{user="4"} 20
			gauge_user{user="5"} 25
	
			# HELP histogram help
			# TYPE histogram histogram
	# No change in the histogram
			histogram_bucket{le="1"} 5
			histogram_bucket{le="3"} 15
			histogram_bucket{le="5"} 25
			histogram_bucket{le="+Inf"} 25
			histogram_sum 75
			histogram_count 25
	
			# HELP histogram_labels help
			# TYPE histogram_labels histogram
	# No change in the histogram per label
			histogram_labels_bucket{label_one="a",le="1"} 5
			histogram_labels_bucket{label_one="a",le="3"} 15
			histogram_labels_bucket{label_one="a",le="5"} 25
			histogram_labels_bucket{label_one="a",le="+Inf"} 25
			histogram_labels_sum{label_one="a"} 75
			histogram_labels_count{label_one="a"} 25
	
			# HELP summary help
			# TYPE summary summary
	# No change in the summary
			summary_sum 75
			summary_count 25
	
			# HELP summary_labels help
			# TYPE summary_labels summary
	# No change in the summary per label
			summary_labels_sum{label_one="a"} 75
			summary_labels_count{label_one="a"} 25
	
			# HELP summary_user help
			# TYPE summary_user summary
	# Summary for user 3 is now missing.
			summary_user_sum{user="1"} 5
			summary_user_count{user="1"} 5
			summary_user_sum{user="2"} 10
			summary_user_count{user="2"} 5
			summary_user_sum{user="4"} 20
			summary_user_count{user="4"} 5
			summary_user_sum{user="5"} 25
			summary_user_count{user="5"} 5
	`)))
}
func TestTenantRegistries_RemoveTenantRegistry_HardRemoval(t *testing.T) {
	tm := setupTestMetrics()

	mainRegistry := prometheus.NewPedanticRegistry()
	mainRegistry.MustRegister(tm)

	// Hard-remove single registry.
	tm.regs.RemoveTenantRegistry(strconv.Itoa(3), true)

	require.NoError(t, testutil.GatherAndCompare(mainRegistry, bytes.NewBufferString(`
			# HELP counter help
			# TYPE counter counter
	# Counter drop (reset!)
			counter 60
	
			# HELP counter_labels help
			# TYPE counter_labels counter
	# Counter drop (reset!)
			counter_labels{label_one="a"} 60
	
			# HELP counter_user help
			# TYPE counter_user counter
	# User 3 is now missing.
			counter_user{user="1"} 5
			counter_user{user="2"} 10
			counter_user{user="4"} 20
			counter_user{user="5"} 25
	
			# HELP gauge help
			# TYPE gauge gauge
	# Drop in the gauge (value 3, counted 5 times)
			gauge 60
	
			# HELP gauge_labels help
			# TYPE gauge_labels gauge
	# Drop in the gauge (value 3, counted 5 times)
			gauge_labels{label_one="a"} 60
	
			# HELP gauge_user help
			# TYPE gauge_user gauge
	# User 3 is now missing.
			gauge_user{user="1"} 5
			gauge_user{user="2"} 10
			gauge_user{user="4"} 20
			gauge_user{user="5"} 25
	
			# HELP histogram help
			# TYPE histogram histogram
	# Histogram drop (reset for sum and count!)
			histogram_bucket{le="1"} 5
			histogram_bucket{le="3"} 10
			histogram_bucket{le="5"} 20
			histogram_bucket{le="+Inf"} 20
			histogram_sum 60
			histogram_count 20
	
			# HELP histogram_labels help
			# TYPE histogram_labels histogram
	# No change in the histogram per label
			histogram_labels_bucket{label_one="a",le="1"} 5
			histogram_labels_bucket{label_one="a",le="3"} 10
			histogram_labels_bucket{label_one="a",le="5"} 20
			histogram_labels_bucket{label_one="a",le="+Inf"} 20
			histogram_labels_sum{label_one="a"} 60
			histogram_labels_count{label_one="a"} 20
	
			# HELP summary help
			# TYPE summary summary
	# Summary drop!
			summary_sum 60
			summary_count 20
	
			# HELP summary_labels help
			# TYPE summary_labels summary
	# Summary drop!
			summary_labels_sum{label_one="a"} 60
			summary_labels_count{label_one="a"} 20
	
			# HELP summary_user help
			# TYPE summary_user summary
	# Summary for user 3 is now missing.
			summary_user_sum{user="1"} 5
			summary_user_count{user="1"} 5
			summary_user_sum{user="2"} 10
			summary_user_count{user="2"} 5
			summary_user_sum{user="4"} 20
			summary_user_count{user="4"} 5
			summary_user_sum{user="5"} 25
			summary_user_count{user="5"} 5
	`)))
}

func TestTenantRegistries_AddUserRegistry_ReplaceRegistry(t *testing.T) {
	tm := setupTestMetrics()

	mainRegistry := prometheus.NewPedanticRegistry()
	mainRegistry.MustRegister(tm)

	// Replace registry for user 5 with empty registry. Replacement does soft-delete previous registry.
	tm.regs.AddTenantRegistry(strconv.Itoa(5), prometheus.NewRegistry())

	require.NoError(t, testutil.GatherAndCompare(mainRegistry, bytes.NewBufferString(`
			# HELP counter help
			# TYPE counter counter
	# No change in counter
			counter 75
	
			# HELP counter_labels help
			# TYPE counter_labels counter
	# No change in counter per label
			counter_labels{label_one="a"} 75
	
			# HELP counter_user help
			# TYPE counter_user counter
	# Per-user counter now missing.
			counter_user{user="1"} 5
			counter_user{user="2"} 10
			counter_user{user="3"} 15
			counter_user{user="4"} 20
	
			# HELP gauge help
			# TYPE gauge gauge
	# Gauge drops by 25 (value for user 5, times 5)
			gauge 50
	
			# HELP gauge_labels help
			# TYPE gauge_labels gauge
	# Gauge drops by 25 (value for user 5, times 5)
			gauge_labels{label_one="a"} 50
	
			# HELP gauge_user help
			# TYPE gauge_user gauge
	# Gauge for user 5 is missing
			gauge_user{user="1"} 5
			gauge_user{user="2"} 10
			gauge_user{user="3"} 15
			gauge_user{user="4"} 20
	
			# HELP histogram help
			# TYPE histogram histogram
	# No change in histogram
			histogram_bucket{le="1"} 5
			histogram_bucket{le="3"} 15
			histogram_bucket{le="5"} 25
			histogram_bucket{le="+Inf"} 25
			histogram_sum 75
			histogram_count 25
	
			# HELP histogram_labels help
			# TYPE histogram_labels histogram
	# No change in histogram per label.
			histogram_labels_bucket{label_one="a",le="1"} 5
			histogram_labels_bucket{label_one="a",le="3"} 15
			histogram_labels_bucket{label_one="a",le="5"} 25
			histogram_labels_bucket{label_one="a",le="+Inf"} 25
			histogram_labels_sum{label_one="a"} 75
			histogram_labels_count{label_one="a"} 25
	
			# HELP summary help
			# TYPE summary summary
	# No change in summary
			summary_sum 75
			summary_count 25
	
			# HELP summary_labels help
			# TYPE summary_labels summary
	# No change in summary per label
			summary_labels_sum{label_one="a"} 75
			summary_labels_count{label_one="a"} 25
	
			# HELP summary_user help
			# TYPE summary_user summary
	# Summary for user 5 now zero (reset)
			summary_user_sum{user="1"} 5
			summary_user_count{user="1"} 5
			summary_user_sum{user="2"} 10
			summary_user_count{user="2"} 5
			summary_user_sum{user="3"} 15
			summary_user_count{user="3"} 5
			summary_user_sum{user="4"} 20
			summary_user_count{user="4"} 5
			summary_user_sum{user="5"} 0
			summary_user_count{user="5"} 0
	`)))
}

func TestTenantRegistries_GetRegistryForTenant(t *testing.T) {
	regs := NewTenantRegistries(log.NewNopLogger())

	assert.Nil(t, regs.GetRegistryForTenant("test"))

	reg1 := prometheus.NewRegistry()
	regs.AddTenantRegistry("test", reg1)

	// Not using assert.Equal, as it compares values that pointers point to.
	assert.True(t, reg1 == regs.GetRegistryForTenant("test"))

	regs.RemoveTenantRegistry("test", false)
	assert.Nil(t, regs.GetRegistryForTenant("test"))

	reg2 := prometheus.NewRegistry()
	regs.AddTenantRegistry("test", reg2)
	assert.True(t, reg2 == regs.GetRegistryForTenant("test"))
	assert.False(t, reg1 == regs.GetRegistryForTenant("test"))

	regs.RemoveTenantRegistry("test", true)
	assert.Nil(t, regs.GetRegistryForTenant("test"))

	reg3 := prometheus.NewRegistry()
	promauto.With(reg3).NewCounter(prometheus.CounterOpts{Name: "test", Help: "test"}).Add(100)
	regs.AddTenantRegistry("test", reg3)

	reg4 := prometheus.NewRegistry()
	regs.AddTenantRegistry("test", reg4) // replaces reg3, which is soft-deleted.
	assert.True(t, reg4 == regs.GetRegistryForTenant("test"))
	assert.False(t, reg3 == regs.GetRegistryForTenant("test"))
	assert.False(t, reg2 == regs.GetRegistryForTenant("test"))
	assert.False(t, reg1 == regs.GetRegistryForTenant("test"))
}

func setupTestMetrics() *testMetrics {
	const numUsers = 5
	const cardinality = 5

	tm := newTestMetrics()

	for userID := 1; userID <= numUsers; userID++ {
		reg := prometheus.NewRegistry()

		labelNames := []string{"label_one", "label_two"}

		g := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{Name: "test_gauge"}, labelNames)
		for i := 0; i < cardinality; i++ {
			g.WithLabelValues("a", strconv.Itoa(i)).Set(float64(userID))
		}

		c := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{Name: "test_counter"}, labelNames)
		for i := 0; i < cardinality; i++ {
			c.WithLabelValues("a", strconv.Itoa(i)).Add(float64(userID))
		}

		h := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{Name: "test_histogram", Buckets: []float64{1, 3, 5}}, labelNames)
		for i := 0; i < cardinality; i++ {
			h.WithLabelValues("a", strconv.Itoa(i)).Observe(float64(userID))
		}

		s := promauto.With(reg).NewSummaryVec(prometheus.SummaryOpts{Name: "test_summary"}, labelNames)
		for i := 0; i < cardinality; i++ {
			s.WithLabelValues("a", strconv.Itoa(i)).Observe(float64(userID))
		}

		tm.regs.AddTenantRegistry(strconv.Itoa(userID), reg)
	}
	return tm
}

type testMetrics struct {
	regs *TenantRegistries

	gauge             *prometheus.Desc
	gaugePerTenant    *prometheus.Desc
	gaugeWithLabels   *prometheus.Desc
	counter           *prometheus.Desc
	counterPerTenant  *prometheus.Desc
	counterWithLabels *prometheus.Desc
	histogram         *prometheus.Desc
	histogramLabels   *prometheus.Desc
	summary           *prometheus.Desc
	summaryPerTenant  *prometheus.Desc
	summaryLabels     *prometheus.Desc
}

func newTestMetrics() *testMetrics {
	return &testMetrics{
		regs: NewTenantRegistries(log.NewNopLogger()),

		gauge:             prometheus.NewDesc("gauge", "help", nil, nil),
		gaugePerTenant:    prometheus.NewDesc("gauge_user", "help", []string{"user"}, nil),
		gaugeWithLabels:   prometheus.NewDesc("gauge_labels", "help", []string{"label_one"}, nil),
		counter:           prometheus.NewDesc("counter", "help", nil, nil),
		counterPerTenant:  prometheus.NewDesc("counter_user", "help", []string{"user"}, nil),
		counterWithLabels: prometheus.NewDesc("counter_labels", "help", []string{"label_one"}, nil),
		histogram:         prometheus.NewDesc("histogram", "help", nil, nil),
		histogramLabels:   prometheus.NewDesc("histogram_labels", "help", []string{"label_one"}, nil),
		summary:           prometheus.NewDesc("summary", "help", nil, nil),
		summaryPerTenant:  prometheus.NewDesc("summary_user", "help", []string{"user"}, nil),
		summaryLabels:     prometheus.NewDesc("summary_labels", "help", []string{"label_one"}, nil),
	}
}

func (tm *testMetrics) Describe(out chan<- *prometheus.Desc) {
	out <- tm.gauge
	out <- tm.gaugePerTenant
	out <- tm.gaugeWithLabels
	out <- tm.counter
	out <- tm.counterPerTenant
	out <- tm.counterWithLabels
	out <- tm.histogram
	out <- tm.histogramLabels
	out <- tm.summary
	out <- tm.summaryPerTenant
	out <- tm.summaryLabels
}

func (tm *testMetrics) Collect(out chan<- prometheus.Metric) {
	data := tm.regs.BuildMetricFamiliesPerTenant()

	data.SendSumOfGauges(out, tm.gauge, "test_gauge")
	data.SendSumOfGaugesPerTenant(out, tm.gaugePerTenant, "test_gauge")
	data.SendSumOfGaugesWithLabels(out, tm.gaugeWithLabels, "test_gauge", "label_one")
	data.SendSumOfCounters(out, tm.counter, "test_counter")
	data.SendSumOfCountersPerTenant(out, tm.counterPerTenant, "test_counter")
	data.SendSumOfCountersWithLabels(out, tm.counterWithLabels, "test_counter", "label_one")
	data.SendSumOfHistograms(out, tm.histogram, "test_histogram")
	data.SendSumOfHistogramsWithLabels(out, tm.histogramLabels, "test_histogram", "label_one")
	data.SendSumOfSummaries(out, tm.summary, "test_summary")
	data.SendSumOfSummariesPerTenant(out, tm.summaryPerTenant, "test_summary")
	data.SendSumOfSummariesWithLabels(out, tm.summaryLabels, "test_summary", "label_one")
}

func collectMetrics(t *testing.T, send func(out chan prometheus.Metric)) []*dto.Metric {
	out := make(chan prometheus.Metric)

	go func() {
		send(out)
		close(out)
	}()

	var metrics []*dto.Metric
	for m := range out {
		collected := &dto.Metric{}
		err := m.Write(collected)
		require.NoError(t, err)

		metrics = append(metrics, collected)
	}

	return metrics
}

func float64p(v float64) *float64 {
	return &v
}

func uint64p(v uint64) *uint64 {
	return &v
}
