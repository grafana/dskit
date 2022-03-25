package ring

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"html/template"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"
)

//go:embed status.gohtml
var defaultPageContent string
var defaultPageTemplate *template.Template

func init() {
	t := template.New("webpage")
	t.Funcs(template.FuncMap{"mod": func(i, j int) bool { return i%j == 0 }})
	defaultPageTemplate = template.Must(t.Parse(defaultPageContent))
}

type ingesterDesc struct {
	ID                  string   `json:"id"`
	State               string   `json:"state"`
	Address             string   `json:"address"`
	HeartbeatTimestamp  string   `json:"timestamp"`
	RegisteredTimestamp string   `json:"registered_timestamp"`
	Zone                string   `json:"zone"`
	Tokens              []uint32 `json:"tokens"`
	NumTokens           int      `json:"-"`
	Ownership           float64  `json:"-"`
}

type httpResponse struct {
	Ingesters  []ingesterDesc `json:"shards"`
	Now        time.Time      `json:"now"`
	ShowTokens bool           `json:"-"`
}

type ringAccess interface {
	casRing(ctx context.Context, f func(in interface{}) (out interface{}, retry bool, err error)) error
	getRing(context.Context) (*Desc, error)
}

type ringPageHandler struct {
	r               ringAccess
	heartbeatPeriod time.Duration
	template        *template.Template
}

func newRingPageHandler(r ringAccess, heartbeatPeriod time.Duration, template *template.Template) *ringPageHandler {
	if template == nil {
		template = defaultPageTemplate
	}
	return &ringPageHandler{
		r:               r,
		heartbeatPeriod: heartbeatPeriod,
		template:        template,
	}
}

func (h *ringPageHandler) handle(w http.ResponseWriter, req *http.Request) {
	if req.Method == http.MethodPost {
		ingesterID := req.FormValue("forget")
		if err := h.forget(req.Context(), ingesterID); err != nil {
			http.Error(
				w,
				fmt.Errorf("error forgetting instance '%s': %w", ingesterID, err).Error(),
				http.StatusInternalServerError,
			)
			return
		}

		// Implement PRG pattern to prevent double-POST and work with CSRF middleware.
		// https://en.wikipedia.org/wiki/Post/Redirect/Get

		// http.Redirect() would convert our relative URL to absolute, which is not what we want.
		// Browser knows how to do that, and it also knows real URL. Furthermore it will also preserve tokens parameter.
		// Note that relative Location URLs are explicitly allowed by specification, so we're not doing anything wrong here.
		w.Header().Set("Location", "#")
		w.WriteHeader(http.StatusFound)

		return
	}

	ringDesc, err := h.r.getRing(req.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, ownedTokens := ringDesc.countTokens()

	ingesterIDs := []string{}
	for id := range ringDesc.Ingesters {
		ingesterIDs = append(ingesterIDs, id)
	}
	sort.Strings(ingesterIDs)

	now := time.Now()
	var ingesters []ingesterDesc
	for _, id := range ingesterIDs {
		ing := ringDesc.Ingesters[id]
		heartbeatTimestamp := time.Unix(ing.Timestamp, 0)
		state := ing.State.String()
		if !ing.IsHealthy(Reporting, h.heartbeatPeriod, now) {
			state = unhealthy
		}

		// Format the registered timestamp.
		registeredTimestamp := ""
		if ing.RegisteredTimestamp != 0 {
			registeredTimestamp = ing.GetRegisteredAt().String()
		}

		ingesters = append(ingesters, ingesterDesc{
			ID:                  id,
			State:               state,
			Address:             ing.Addr,
			HeartbeatTimestamp:  heartbeatTimestamp.String(),
			RegisteredTimestamp: registeredTimestamp,
			Tokens:              ing.Tokens,
			Zone:                ing.Zone,
			NumTokens:           len(ing.Tokens),
			Ownership:           (float64(ownedTokens[id]) / float64(math.MaxUint32)) * 100,
		})
	}

	tokensParam := req.URL.Query().Get("tokens")

	renderHTTPResponse(w, httpResponse{
		Ingesters:  ingesters,
		Now:        now,
		ShowTokens: tokensParam == "true",
	}, h.template, req)
}

// RenderHTTPResponse either responds with json or a rendered html page using the passed in template
// by checking the Accepts header
func renderHTTPResponse(w http.ResponseWriter, v httpResponse, t *template.Template, r *http.Request) {
	accept := r.Header.Get("Accept")
	if strings.Contains(accept, "application/json") {
		writeJSONResponse(w, v)
		return
	}

	err := t.Execute(w, v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (h *ringPageHandler) forget(ctx context.Context, id string) error {
	unregister := func(in interface{}) (out interface{}, retry bool, err error) {
		if in == nil {
			return nil, false, fmt.Errorf("found empty ring when trying to unregister")
		}

		ringDesc := in.(*Desc)
		ringDesc.RemoveIngester(id)
		return ringDesc, true, nil
	}
	return h.r.casRing(ctx, unregister)
}

// WriteJSONResponse writes some JSON as a HTTP response.
func writeJSONResponse(w http.ResponseWriter, v httpResponse) {
	w.Header().Set("Content-Type", "application/json")

	data, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// We ignore errors here, because we cannot do anything about them.
	// Write will trigger sending Status code, so we cannot send a different status code afterwards.
	// Also this isn't internal error, but error communicating with client.
	_, _ = w.Write(data)
}
