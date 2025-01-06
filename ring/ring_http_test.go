package ring

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRingPageHandler_handle(t *testing.T) {
	now := time.Now()
	ring := fakeRingAccess{
		desc: &Desc{
			Ingesters: map[string]*InstanceDesc{
				"1": {
					Zone:      "zone-a",
					State:     InstanceState_ACTIVE,
					Addr:      "addr-a",
					Timestamp: now.Unix(),
					Tokens:    []uint32{1000000, 3000000, 6000000},
				},
				"2": {
					Zone:      "zone-b",
					State:     InstanceState_ACTIVE,
					Addr:      "addr-b",
					Timestamp: now.Unix(),
					Tokens:    []uint32{2000000, 4000000, 5000000, 7000000},
				},
			},
		},
	}
	handler := newRingPageHandler(&ring, 10*time.Second, false)

	t.Run("displays instance info", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		handler.handle(recorder, httptest.NewRequest(http.MethodGet, "/ring", nil))

		assert.Equal(t, http.StatusOK, recorder.Code)
		assert.Equal(t, "text/html", recorder.Header().Get("Content-Type"))

		assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("(?m)%s", strings.Join([]string{
			"<td>", "1", "</td>",
			"<td>", "zone-a", "</td>",
			"<td>", "InstanceState_ACTIVE", "</td>",
			"<td>", "addr-a", "</td>",
		}, `\s*`))), recorder.Body.String())

		assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("(?m)%s", strings.Join([]string{
			"<td>", "3", "</td>",
			"<td>", "100%", "</td>",
		}, `\s*`))), recorder.Body.String())

		assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("(?m)%s", strings.Join([]string{
			"<td>", "2", "</td>",
			"<td>", "zone-b", "</td>",
			"<td>", "InstanceState_ACTIVE", "</td>",
			"<td>", "addr-b", "</td>",
		}, `\s*`))), recorder.Body.String())

		assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("(?m)%s", strings.Join([]string{
			"<td>", "4", "</td>",
			"<td>", "100%", "</td>",
		}, `\s*`))), recorder.Body.String())
	})

	t.Run("displays Show Tokens button by default", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		handler.handle(recorder, httptest.NewRequest(http.MethodGet, "/ring", nil))

		assert.Equal(t, http.StatusOK, recorder.Code)
		assert.Equal(t, "text/html", recorder.Header().Get("Content-Type"))

		assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("(?m)%s", strings.Join([]string{
			`<input type="button" value="Show Tokens" onclick="window.location.href = '\?tokens=true'"/>`,
		}, `\s*`))), recorder.Body.String())
	})

	t.Run("displays tokens when Show Tokens is enabled", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		handler.handle(recorder, httptest.NewRequest(http.MethodGet, "/ring?tokens=true", nil))

		assert.Equal(t, http.StatusOK, recorder.Code)
		assert.Equal(t, "text/html", recorder.Header().Get("Content-Type"))

		assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("(?m)%s", strings.Join([]string{
			`<input type="button" value="Hide Tokens" onclick="window.location.href = '\?tokens=false' "/>`,
		}, `\s*`))), recorder.Body.String())

		assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("(?m)%s", strings.Join([]string{
			"<h2>", "Instance: 1", "</h2>",
			"<p>", "Tokens:<br/>", "1000000", "3000000", "6000000", "</p>",
		}, `\s*`))), recorder.Body.String())

		assert.Regexp(t, regexp.MustCompile(fmt.Sprintf("(?m)%s", strings.Join([]string{
			"<h2>", "Instance: 2", "</h2>",
			"<p>", "Tokens:<br/>", "2000000", "4000000", "5000000", "7000000", "</p>",
		}, `\s*`))), recorder.Body.String())
	})

	tokenDisabledHandler := newRingPageHandler(&ring, 10*time.Second, true)

	t.Run("hides token columns when tokens are disabled", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		tokenDisabledHandler.handle(recorder, httptest.NewRequest(http.MethodGet, "/ring", nil))

		assert.Equal(t, http.StatusOK, recorder.Code)
		assert.Equal(t, "text/html", recorder.Header().Get("Content-Type"))

		assert.NotRegexp(t, regexp.MustCompile(fmt.Sprintf("(?m)%s", strings.Join([]string{
			"<th>", "Tokens", "</th>",
			"<th>", "Ownership", "</th>",
		}, `\s*`))), recorder.Body.String())

		assert.NotRegexp(t, regexp.MustCompile(fmt.Sprintf("(?m)%s", strings.Join([]string{
			"<td>", "3", "</td>",
			"<td>", "100%", "</td>",
		}, `\s*`))), recorder.Body.String())

		assert.NotRegexp(t, regexp.MustCompile(fmt.Sprintf("(?m)%s", strings.Join([]string{
			"<td>", "4", "</td>",
			"<td>", "100%", "</td>",
		}, `\s*`))), recorder.Body.String())
	})

	t.Run("hides Show Tokens button when tokens are disabled", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		tokenDisabledHandler.handle(recorder, httptest.NewRequest(http.MethodGet, "/ring", nil))

		assert.Equal(t, http.StatusOK, recorder.Code)
		assert.Equal(t, "text/html", recorder.Header().Get("Content-Type"))

		assert.NotRegexp(t, regexp.MustCompile(fmt.Sprintf("(?m)%s", strings.Join([]string{
			`input type="button" value="Show Tokens"`,
		}, `\s*`))), recorder.Body.String())
	})
}

type fakeRingAccess struct {
	desc *Desc
}

func (f *fakeRingAccess) getRing(context.Context) (*Desc, error) {
	return f.desc, nil
}

func (f *fakeRingAccess) casRing(_ context.Context, _ func(in interface{}) (out interface{}, retry bool, err error)) error {
	return nil
}
