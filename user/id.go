// Provenance-includes-location: https://github.com/weaveworks/common/blob/main/user/id.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Weaveworks Ltd.

package user

import (
	"context"
	"errors"
)

type contextKey int

const (
	// Keys used in contexts to find the org or user ID
	orgIDContextKey  contextKey = 0
	userIDContextKey contextKey = 1
)

// boxedOrgIDContextKey is orgIDContextKey pre-boxed as an interface{} to avoid
// repeated heap allocations when used as a key in context.Value lookups and
// comparisons. Without this, each call to ctx.Value(orgIDContextKey) would box
// the contextKey value on the heap.
var boxedOrgIDContextKey any = orgIDContextKey

// Errors that we return
var (
	ErrNoOrgID               = errors.New("no org id")
	ErrDifferentOrgIDPresent = errors.New("different org ID already present")
	ErrTooManyOrgIDs         = errors.New("multiple org IDs present")

	ErrNoUserID               = errors.New("no user id")
	ErrDifferentUserIDPresent = errors.New("different user ID already present")
)

// ExtractOrgID gets the org ID from the context.
func ExtractOrgID(ctx context.Context) (string, error) {
	// Fast path: check if the context is our optimized OrgIDCtx type first,
	// avoiding the allocation from boxing the return value of Value() as any.
	if c, ok := ctx.(*OrgIDCtx); ok {
		return c.orgID, nil
	}
	orgID, ok := ctx.Value(boxedOrgIDContextKey).(string)
	if !ok {
		return "", ErrNoOrgID
	}
	return orgID, nil
}

// InjectOrgID returns a derived context containing the org ID.
func InjectOrgID(ctx context.Context, orgID string) context.Context {
	return context.WithValue(ctx, boxedOrgIDContextKey, orgID)
}

// OrgIDCtx is a context.Context implementation that stores an org ID inline,
// avoiding the allocation overhead of context.WithValue. It intercepts
// Value calls for the org ID context key and delegates everything else to the
// embedded parent context.
//
// This type is intended to be embedded in structs to avoid a separate heap
// allocation when only the org ID needs to be injected into the context chain.
type OrgIDCtx struct {
	context.Context
	orgID string
}

// SetOrgID sets the org ID in this context.
func (c *OrgIDCtx) SetOrgID(orgID string) {
	c.orgID = orgID
}

// Value intercepts lookups for the org ID context key and returns the stored
// org ID. All other keys are delegated to the parent context.
func (c *OrgIDCtx) Value(key any) any {
	if key == boxedOrgIDContextKey {
		return c.orgID
	}
	return c.Context.Value(key)
}

// InjectOrgIDInline returns a context with the org ID injected using an inline
// context type, avoiding the separate allocation that context.WithValue makes.
// The returned context supports extraction via ExtractOrgID.
func InjectOrgIDInline(ctx context.Context, orgID string) context.Context {
	return &OrgIDCtx{Context: ctx, orgID: orgID}
}

// ExtractUserID gets the user ID from the context.
func ExtractUserID(ctx context.Context) (string, error) {
	userID, ok := ctx.Value(userIDContextKey).(string)
	if !ok {
		return "", ErrNoUserID
	}
	return userID, nil
}

// InjectUserID returns a derived context containing the user ID.
func InjectUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, interface{}(userIDContextKey), userID)
}
