// SPDX-License-Identifier: AGPL-3.0-only

// Package propagation provides abstractions for propagating auxiliary request information
// across both HTTP and protobuf transport paths.
//
// Two transport paths are typically used when sending requests between query-frontends,
// query-schedulers, and queriers:
//   - httpgrpc (wraps an HTTP request inside gRPC)
//   - Protobuf (a direct protobuf encoding for the newer remote execution path)
//
// Rather than implementing propagation logic twice for each path, this package defines
// Extractor, Injector, and Carrier interfaces that abstract over the two.
package propagation

import (
	"context"
	"net/http"
	"net/textproto"
)

// Extractor extracts auxiliary information from a request carrier into the context.
type Extractor interface {
	ExtractFromCarrier(ctx context.Context, carrier Carrier) (context.Context, error)
}

// Injector injects auxiliary information from the context into a request carrier.
type Injector interface {
	InjectToCarrier(ctx context.Context, carrier Carrier) error
}

// Carrier represents a carrier of key-value pairs for a request, such as HTTP headers.
// Keys are canonicalized by textproto.CanonicalMIMEHeaderKey.
type Carrier interface {
	// Get returns the first value with the given name, or an empty string if not present.
	Get(name string) string

	// GetAll returns all values with the given name, or nil if not present.
	GetAll(name string) []string

	// Add adds value to the existing values stored for name.
	Add(name, value string)

	// SetAll replaces all values stored for name with the provided slice.
	SetAll(name string, value []string)
}

// NoopExtractor is an Extractor that does nothing.
type NoopExtractor struct{}

func (e *NoopExtractor) ExtractFromCarrier(ctx context.Context, _ Carrier) (context.Context, error) {
	return ctx, nil
}

// NoopInjector is an Injector that does nothing.
type NoopInjector struct{}

func (i *NoopInjector) InjectToCarrier(_ context.Context, _ Carrier) error {
	return nil
}

// MultiExtractor chains multiple Extractors, applying each in order.
type MultiExtractor struct {
	Extractors []Extractor
}

func (m *MultiExtractor) ExtractFromCarrier(ctx context.Context, carrier Carrier) (context.Context, error) {
	for _, e := range m.Extractors {
		var err error
		ctx, err = e.ExtractFromCarrier(ctx, carrier)
		if err != nil {
			return nil, err
		}
	}
	return ctx, nil
}

// MultiInjector chains multiple Injectors, applying each in order.
type MultiInjector struct {
	Injectors []Injector
}

func (m *MultiInjector) InjectToCarrier(ctx context.Context, carrier Carrier) error {
	for _, i := range m.Injectors {
		if err := i.InjectToCarrier(ctx, carrier); err != nil {
			return err
		}
	}
	return nil
}

// MapCarrier is a map-based Carrier implementation backed by a map[string][]string.
// Keys are canonicalized by textproto.CanonicalMIMEHeaderKey.
type MapCarrier map[string][]string

func (m MapCarrier) Get(name string) string {
	if values := m.GetAll(name); len(values) > 0 {
		return values[0]
	}
	return ""
}

func (m MapCarrier) GetAll(name string) []string {
	return m[textproto.CanonicalMIMEHeaderKey(name)]
}

func (m MapCarrier) Add(name, value string) {
	key := textproto.CanonicalMIMEHeaderKey(name)
	m[key] = append(m[key], value)
}

func (m MapCarrier) SetAll(name string, value []string) {
	m[textproto.CanonicalMIMEHeaderKey(name)] = value
}

// HttpHeaderCarrier wraps http.Header to implement Carrier.
type HttpHeaderCarrier http.Header

func (h HttpHeaderCarrier) Get(name string) string {
	return http.Header(h).Get(name)
}

func (h HttpHeaderCarrier) GetAll(name string) []string {
	return http.Header(h).Values(name)
}

func (h HttpHeaderCarrier) Add(name, value string) {
	http.Header(h).Add(name, value)
}

func (h HttpHeaderCarrier) SetAll(name string, value []string) {
	h[textproto.CanonicalMIMEHeaderKey(name)] = value
}
