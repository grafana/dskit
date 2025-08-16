package grpcutil

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/tap"
)

type ctxKey int

const (
	ctxKey1 ctxKey = 1
	ctxKey2 ctxKey = 2
)

func TestComposeTapHandles(t *testing.T) {
	t.Run("single handle is not composed", func(t *testing.T) {
		called := false
		h := func(ctx context.Context, info *tap.Info) (context.Context, error) {
			called = true
			return ctx, nil
		}

		composed := ComposeTapHandles([]tap.ServerInHandle{h})

		ctx, err := composed(context.Background(), &tap.Info{})
		assert.NoError(t, err)
		assert.NotNil(t, ctx)
		assert.True(t, called)
	})

	t.Run("multiple handles are composed", func(t *testing.T) {
		var results []int
		h1 := func(ctx context.Context, info *tap.Info) (context.Context, error) {
			results = append(results, 1)
			return context.WithValue(ctx, ctxKey1, true), nil
		}
		h2 := func(ctx context.Context, info *tap.Info) (context.Context, error) {
			results = append(results, 2)
			return context.WithValue(ctx, ctxKey2, true), nil
		}

		composed := ComposeTapHandles([]tap.ServerInHandle{h1, h2})

		ctx, err := composed(context.Background(), &tap.Info{})
		assert.NoError(t, err)
		assert.True(t, ctx.Value(ctxKey1).(bool))
		assert.True(t, ctx.Value(ctxKey2).(bool))
		assert.Equal(t, []int{1, 2}, results, "handles should be called in results")
	})

	t.Run("error short-circuits execution", func(t *testing.T) {
		var results []int
		h1 := func(ctx context.Context, info *tap.Info) (context.Context, error) {
			results = append(results, 1)
			return ctx, errors.New("boom")
		}
		h2 := func(ctx context.Context, info *tap.Info) (context.Context, error) {
			results = append(results, 2)
			return ctx, nil
		}

		composed := ComposeTapHandles([]tap.ServerInHandle{h1, h2})

		ctx, err := composed(context.Background(), &tap.Info{})
		assert.Error(t, err)
		assert.EqualError(t, err, "boom")
		assert.NotNil(t, ctx)
		assert.Equal(t, []int{1}, results, "second handle should not be called after error")
	})
}
